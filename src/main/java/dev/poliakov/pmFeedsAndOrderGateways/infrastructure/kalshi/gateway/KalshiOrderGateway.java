package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.gateway;

import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Order;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.OrderResult;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.OrderType;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Token;
import dev.poliakov.pmFeedsAndOrderGateways.domain.port.out.OrderGateway;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest.KalshiTradeApi;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest.model.KalshiOrderResponse;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest.model.KalshiSendOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class KalshiOrderGateway implements OrderGateway {

    private static final Logger log = LoggerFactory.getLogger(KalshiOrderGateway.class);

    private static final String SIDE_BID = "bid";
    private static final String SIDE_ASK = "ask";

    private static final String SELF_TRADE_PREVENTION = "taker_at_cross";

    private static final int COUNT_SCALE = 2;
    private static final int PRICE_SCALE = 4;

    private final KalshiTradeApi api;

    public KalshiOrderGateway(KalshiTradeApi api) {
        this.api = api;
    }

    private static void logMapping(List<Order> orders, List<KalshiSendOrder> sendOrders) {
        for (int i = 0; i < orders.size() && i < sendOrders.size(); i++) {
            Order order = orders.get(i);
            KalshiSendOrder wire = sendOrders.get(i);
            log.info("Kalshi leg mapped: {} {} size={} (our book={} limit={}) -> side={} price={}",
                    order.token().label(), order.side(), order.size(),
                    order.bookPrice(), order.price(), wire.side(), wire.price());
        }
    }

    private static KalshiSendOrder buildSendOrder(Order order, OrderType orderType) {
        if (!(order.token() instanceof Token.Kalshi(String marketTicker, Token.Outcome outcome))) {
            throw new IllegalArgumentException(
                    "KalshiOrderGateway received a non-Kalshi leg: " + order.token());
        }
        boolean yes = outcome == Token.Outcome.YES;
        boolean buy = order.side() == Order.Side.BUY;
        String side = (yes == buy) ? SIDE_BID : SIDE_ASK;
        double wirePrice = yes ? order.price() : 1.0 - order.price();

        String timeInForce = switch (orderType) {
            case FOK -> "fill_or_kill";
            case FAK -> "immediate_or_cancel";
        };

        return new KalshiSendOrder(
                marketTicker,
                UUID.randomUUID().toString(),
                side,
                fixedPoint(order.size(), COUNT_SCALE),
                fixedPoint(wirePrice, PRICE_SCALE),
                timeInForce,
                SELF_TRADE_PREVENTION);
    }

    private static String fixedPoint(double value, int scale) {
        return BigDecimal.valueOf(value).setScale(scale, RoundingMode.HALF_UP).toPlainString();
    }

    private static List<OrderResult> zipResults(List<Order> orders, List<KalshiOrderResponse> responses) {
        if (responses.size() != orders.size()) {
            log.error("Kalshi batch response count {} != request count {} — cannot attribute every "
                    + "leg's fill, treating missing legs as unfilled", responses.size(), orders.size());
        }
        List<OrderResult> out = new ArrayList<>(orders.size());
        for (int i = 0; i < orders.size(); i++) {
            out.add(i < responses.size()
                    ? toResult(responses.get(i))
                    : new OrderResult(null, OrderResult.Status.REJECTED, 0.0));
        }
        return out;
    }

    private static OrderResult toResult(KalshiOrderResponse res) {
        if (res.error() != null) {
            log.error("Kalshi leg rejected: code={} message={} details={}",
                    res.error().code(), res.error().message(), res.error().details());
            return new OrderResult(res.orderId(), OrderResult.Status.REJECTED, 0.0);
        }
        double filled = parseFixedPoint(res.fillCount());
        double remaining = parseFixedPoint(res.remainingCount());
        OrderResult.Status status = remaining > 0
                ? OrderResult.Status.LIVE
                : OrderResult.Status.MATCHED;
        return new OrderResult(res.orderId(), status, filled);
    }

    private static double parseFixedPoint(String value) {
        if (value == null || value.isBlank()) {
            return 0.0;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            log.error("Kalshi returned an unparseable fixed-point value: '{}'", value);
            return 0.0;
        }
    }

    @Override
    public Mono<OrderResult> placeOrder(Order order, OrderType orderType) {
        return Mono.fromCallable(() -> buildSendOrder(order, orderType))
                .flatMap(api::postOrder)
                .map(KalshiOrderGateway::toResult);
    }

    @Override
    public Mono<List<OrderResult>> placeOrders(List<Order> orders, OrderType orderType) {
        return Mono.fromCallable(() -> orders.stream()
                        .map(order -> buildSendOrder(order, orderType))
                        .toList())
                .doOnNext(sendOrders -> logMapping(orders, sendOrders))
                .flatMap(api::postOrders)
                .map(responses -> zipResults(orders, responses));
    }

    @Override
    public Mono<Void> cancel(String orderId) {
        return api.cancelOrder(orderId);
    }

    @Override
    public Mono<Void> warmup() {
        return api.warmup();
    }
}

