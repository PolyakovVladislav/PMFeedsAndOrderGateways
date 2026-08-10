package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.gateway;

import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.*;
import dev.poliakov.pmFeedsAndOrderGateways.domain.port.out.OrderGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.*;

public class RoutingOrderGateway implements OrderGateway {

    private static final Logger log = LoggerFactory.getLogger(RoutingOrderGateway.class);

    private final Map<Exchange, OrderGateway> gateways;

    public RoutingOrderGateway(OrderGateway polymarketOrderGateway, OrderGateway kalshiOrderGateway,
                               OrderGateway predictFunOrderGateway) {
        this(Map.of(
                Exchange.POLYMARKET, polymarketOrderGateway,
                Exchange.KALSHI, kalshiOrderGateway,
                Exchange.PREDICTFUN, predictFunOrderGateway));
    }

    // Route across only the venues actually wired. A leg for a venue that was not configured hits
    // gatewayFor's IllegalStateException below — the honest outcome, since the client was never
    // given credentials for it and must not silently pretend to trade there.
    public RoutingOrderGateway(Map<Exchange, OrderGateway> gateways) {
        this.gateways = Map.copyOf(gateways);
    }

    private static List<OrderResult> merge(int legCount, Object[] batches) {
        OrderResult[] merged = new OrderResult[legCount];
        for (Object element : batches) {
            LegBatch batch = (LegBatch) element;
            List<Integer> indices = batch.indices();
            List<OrderResult> results = batch.results();
            if (results.size() != indices.size()) {
                log.error("{} returned {} results for {} legs — unmatched legs count as unfilled",
                        batch.exchange(), results.size(), indices.size());
            }
            for (int i = 0; i < indices.size(); i++) {
                merged[indices.get(i)] = i < results.size()
                        ? results.get(i)
                        : new OrderResult(null, OrderResult.Status.REJECTED, 0.0);
            }
        }
        for (int i = 0; i < merged.length; i++) {
            if (merged[i] == null) {
                log.error("Leg {} got no result from any exchange — counting it as unfilled", i);
                merged[i] = new OrderResult(null, OrderResult.Status.REJECTED, 0.0);
            }
        }
        return Arrays.asList(merged);
    }

    @Override
    public Mono<OrderResult> placeOrder(Order order, OrderType orderType) {
        return gatewayFor(order.token().exchange()).placeOrder(order, orderType);
    }

    @Override
    public Mono<Double> resolveFilledSize(Token token, String orderId) {
        return gatewayFor(token.exchange()).resolveFilledSize(token, orderId);
    }

    @Override
    public Mono<List<OrderResult>> placeOrders(List<Order> orders, OrderType orderType) {
        if (orders.isEmpty()) {
            return Mono.just(List.of());
        }

        Map<Exchange, List<Integer>> legsByExchange = new LinkedHashMap<>();
        for (int i = 0; i < orders.size(); i++) {
            legsByExchange
                    .computeIfAbsent(orders.get(i).token().exchange(), e -> new ArrayList<>())
                    .add(i);
        }

        if (legsByExchange.size() == 1) {
            Exchange only = legsByExchange.keySet().iterator().next();
            return gatewayFor(only).placeOrders(orders, orderType);
        }

        List<Mono<LegBatch>> calls = new ArrayList<>(legsByExchange.size());
        legsByExchange.forEach((exchange, indices) -> {
            List<Order> subset = indices.stream().map(orders::get).toList();
            calls.add(gatewayFor(exchange)
                    .placeOrders(subset, orderType)
                    .map(results -> new LegBatch(exchange, indices, results))

                    .onErrorResume(err -> {
                        log.error("{} leg(s) of a cross-exchange combo failed outright — treating them "
                                + "as unfilled; the other exchange may still have executed, "
                                + "check for a naked position", exchange, err);
                        return Mono.just(new LegBatch(exchange, indices, List.of()));
                    }));
        });

        log.info("Cross-exchange combo: {} legs across {} — sending one bulk request per exchange "
                        + "concurrently (not atomic, fills must be verified)",
                orders.size(), legsByExchange.keySet());

        return Mono.zip(calls, batches -> merge(orders.size(), batches));
    }

    @Override
    public Mono<Void> cancel(String orderId) {
        if (orderId == null || orderId.isBlank()) {
            return Mono.error(new IllegalArgumentException("cancel() needs an order id"));
        }
        if (orderId.startsWith("0x")) {
            return gatewayFor(Exchange.POLYMARKET).cancel(orderId);
        }
        try {
            UUID.fromString(orderId);
        } catch (IllegalArgumentException e) {
            return Mono.error(new IllegalArgumentException(
                    "Cannot tell which exchange order id '" + orderId + "' belongs to"));
        }
        return gatewayFor(Exchange.KALSHI).cancel(orderId);
    }

    @Override
    public Mono<Void> warmup() {
        return Mono.when(gateways.values().stream().map(OrderGateway::warmup).toList());
    }

    @Override
    public Mono<Void> prepare(List<Token> tokens) {
        return Mono.when(gateways.values().stream()
                .map(gateway -> gateway.prepare(tokens))
                .toList());
    }

    private OrderGateway gatewayFor(Exchange exchange) {
        OrderGateway gateway = gateways.get(exchange);
        if (gateway == null) {
            throw new IllegalStateException("No order gateway wired for exchange " + exchange);
        }
        return gateway;
    }

    private record LegBatch(Exchange exchange, List<Integer> indices, List<OrderResult> results) {
    }
}

