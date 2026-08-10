package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.gateway;

import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Order;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.OrderResult;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.OrderType;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Token;
import dev.poliakov.pmFeedsAndOrderGateways.domain.port.out.OrderGateway;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.PolymarketClobApi;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.PolymarketCredentials;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.PolymarketNegRiskRegistry;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.model.ClobOrder;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.model.OrderResponse;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.model.SendOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class PolymarketOrderGateway implements OrderGateway {

    private static final Logger log = LoggerFactory.getLogger(PolymarketOrderGateway.class);

    private static final long USDC_DECIMALS = 1_000_000L;
    private static final long USDC_PRECISION_UNIT = USDC_DECIMALS / 100;
    private static final long TOKEN_PRECISION_UNIT = USDC_DECIMALS / 10_000;
    private static final int SIGNATURE_TYPE = 3;

    private static final String BYTES32_ZERO =
            "0x0000000000000000000000000000000000000000000000000000000000000000";

    private static final int RESOLVE_POLLS = 8;
    private static final java.time.Duration RESOLVE_POLL_GAP = java.time.Duration.ofSeconds(1);

    private final PolymarketClobApi api;
    private final PolymarketCredentials credentials;
    private final PolymarketNegRiskRegistry negRiskRegistry;
    private final Eip712OrderSigner signer;
    private final Eip712OrderSigner negRiskSigner;

    public PolymarketOrderGateway(PolymarketClobApi api, PolymarketCredentials credentials,
                                  PolymarketNegRiskRegistry negRiskRegistry) {
        this.api = api;
        this.credentials = credentials;
        this.negRiskRegistry = negRiskRegistry;
        this.signer = new Eip712OrderSigner(
                credentials.privateKey(), credentials.exchangeContract());
        this.negRiskSigner = new Eip712OrderSigner(
                credentials.privateKey(), credentials.negRiskExchangeContract());
    }

    private static List<String> assetIds(List<Order> orders) {
        return assetIds(orders.stream().map(Order::token));
    }

    private static List<String> assetIds(Stream<Token> tokens) {
        return tokens.filter(Token.Polymarket.class::isInstance)
                .map(token -> ((Token.Polymarket) token).assetId())
                .distinct()
                .toList();
    }

    private static List<OrderResult> zipResults(List<Order> orders, List<OrderResponse> responses) {
        if (responses.size() != orders.size()) {
            log.warn("Bulk order response count {} != request count {} — pairing sides by index",
                    responses.size(), orders.size());
        }
        List<OrderResult> out = new ArrayList<>(responses.size());
        for (int i = 0; i < responses.size(); i++) {
            Order.Side side = orders.get(Math.min(i, orders.size() - 1)).side();
            out.add(toResult(responses.get(i), side));
        }
        return out;
    }

    private static BigInteger[] toAmounts(Order order) {
        if (order.side() == Order.Side.BUY) {
            long usdc = floorTo(Math.round(order.bookPrice() * order.size() * USDC_DECIMALS),
                    USDC_PRECISION_UNIT);
            long minTokens = floorTo(Math.round(usdc / order.price()), TOKEN_PRECISION_UNIT);
            return new BigInteger[]{BigInteger.valueOf(usdc), BigInteger.valueOf(minTokens)};
        }
        long tokens = floorTo(Math.round(order.size() * USDC_DECIMALS), TOKEN_PRECISION_UNIT);
        long usdc = floorTo(Math.round(order.price() * order.size() * USDC_DECIMALS), USDC_PRECISION_UNIT);

        if (usdc == 0) usdc = USDC_PRECISION_UNIT;
        return new BigInteger[]{BigInteger.valueOf(tokens), BigInteger.valueOf(usdc)};
    }

    private static long floorTo(long atomicAmount, long unit) {
        return (atomicAmount / unit) * unit;
    }

    private static OrderResult toResult(OrderResponse res, Order.Side side) {
        OrderResult.Status status = switch (res.status() == null ? "" : res.status()) {
            case "matched" -> OrderResult.Status.MATCHED;
            case "delayed" -> OrderResult.Status.DELAYED;
            case "live" -> OrderResult.Status.LIVE;
            default -> OrderResult.Status.REJECTED;
        };
        if (status == OrderResult.Status.REJECTED) {
            // errorMsg is the only place the venue says *why*, and it was parsed and then dropped:
            // OrderResult carries no room for it, so a rejection reached the log as nothing but an
            // empty orderId. Five silent rejections in a row is exactly the moment that reason is
            // worth having — a rejected leg beside a filled one is how a naked position appears.
            log.warn("Polymarket rejected an order: status={} success={} errorMsg={} orderId={}",
                    res.status(), res.success(), res.errorMsg(), res.orderId());
        }
        String tokenField = side == Order.Side.BUY ? res.takingAmount() : res.makingAmount();
        double filled = tokenField != null && !tokenField.isBlank()
                ? Double.parseDouble(tokenField)
                : 0.0;
        return new OrderResult(res.orderId(), status, filled);
    }

    @Override
    public Mono<Double> resolveFilledSize(Token token, String orderId) {
        if (!(token instanceof Token.Polymarket) || orderId == null || orderId.isBlank()) {
            return Mono.empty();
        }
        // Whether any poll got a real order state back, as opposed to the venue not admitting the
        // order exists yet. That distinction decides what an exhausted window means, and collapsing
        // the two is the difference between a correct position and a doubled one — see below.
        AtomicBoolean answered = new AtomicBoolean(false);
        // An explicit walk over the poll budget rather than repeatWhenEmpty, which does NOT complete
        // empty when its budget runs out — it raises "Exceeded maximum number of repeats". The
        // defaultIfEmpty(0.0) that used to sit under it was therefore unreachable, and every order
        // that genuinely matched nothing was escalated to manual reconciliation as if unresolvable.
        // concatMap keeps the polls strictly sequential and next() cancels the rest on the first hit.
        return Flux.range(0, RESOLVE_POLLS)
                .concatMap(attempt -> Mono.defer(() -> api.getOrder(orderId))
                        .doOnNext(state -> answered.set(true))
                        .map(state -> {
                            double matched = state.sizeMatchedOrZero();
                            log.info("Polymarket order {} state poll {}/{}: status={} size_matched={}",
                                    orderId, attempt + 1, RESOLVE_POLLS, state.status(), matched);
                            return matched;
                        })
                        .filter(matched -> matched > 0)
                        .delaySubscription(attempt == 0
                                ? java.time.Duration.ZERO
                                : RESOLVE_POLL_GAP))
                .next()
                .switchIfEmpty(Mono.defer(() -> answered.get()
                        // Every poll saw a real state and none of them had matched anything: for a
                        // FAK order that is the answer, not a timeout — it never matched.
                        ? Mono.just(0.0)
                        // The window expired without the venue ever acknowledging the order. Zero
                        // would be a guess, and the dangerous one: the caller would write this leg
                        // down to nothing, recovery would buy it a second time, and a late match
                        // would leave twice the intended position with no hedge. Failing keeps the
                        // assumed size and hands it to a human, which is recoverable.
                        : Mono.error(new IllegalStateException(
                        "Polymarket never returned a state for order " + orderId
                                + " within the poll window — it may still match"))))
                .doOnError(err -> log.error("Polymarket order {} could not be resolved — the recorded "
                        + "position for this leg is still the assumed size and needs reconciling by "
                        + "hand", orderId, err));
    }

    @Override
    public Mono<OrderResult> placeOrder(Order order, OrderType orderType) {
        return negRiskRegistry.resolve(assetIds(List.of(order)))
                .publishOn(Schedulers.boundedElastic())
                .map(negRiskByAssetId -> buildSendOrder(order, orderType, negRiskByAssetId))
                .flatMap(api::postOrder)
                .map(res -> toResult(res, order.side()));
    }

    @Override
    public Mono<List<OrderResult>> placeOrders(List<Order> orders, OrderType orderType) {

        return negRiskRegistry.resolve(assetIds(orders))

                .publishOn(Schedulers.boundedElastic())
                .map(negRiskByAssetId -> orders.stream()
                        .map(order -> buildSendOrder(order, orderType, negRiskByAssetId))
                        .toList())
                .flatMap(api::postOrders)
                .map(responses -> zipResults(orders, responses));
    }

    @Override
    public Mono<Void> prepare(List<Token> tokens) {
        return negRiskRegistry.prefetch(assetIds(tokens.stream()));
    }

    @Override
    public Mono<Void> cancel(String orderId) {
        return api.cancelOrder(orderId);
    }

    @Override
    public Mono<Void> warmup() {
        Mono<Void> resolveCredentials = Mono.fromCallable(credentials::apiKey)
                .subscribeOn(Schedulers.boundedElastic())
                .then();
        return Mono.when(api.warmup(), resolveCredentials);
    }

    private SendOrder buildSendOrder(Order order, OrderType orderType,
                                     Map<String, Boolean> negRiskByAssetId) {
        if (!(order.token() instanceof Token.Polymarket(String assetId))) {
            throw new IllegalArgumentException(
                    "PolymarketOrderGateway received a non-Polymarket leg: " + order.token());
        }
        Boolean negRisk = negRiskByAssetId.get(assetId);
        if (negRisk == null) {
            throw new IllegalStateException(
                    "No neg-risk flag resolved for Polymarket token " + assetId);
        }
        int salt = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
        long timestamp = System.currentTimeMillis();
        int side = order.side() == Order.Side.BUY ? 0 : 1;

        BigInteger[] amounts = toAmounts(order);
        BigInteger makerAmount = amounts[0];
        BigInteger takerAmount = amounts[1];

        String signature = (negRisk ? negRiskSigner : signer).sign(
                salt,
                credentials.address(),
                credentials.address(),
                assetId,
                makerAmount,
                takerAmount,
                timestamp,
                side,
                SIGNATURE_TYPE);

        ClobOrder clobOrder = new ClobOrder(
                credentials.address(),
                credentials.address(),
                assetId,
                makerAmount.toString(),
                takerAmount.toString(),
                order.side().name(),
                "0",
                String.valueOf(timestamp),
                BYTES32_ZERO,
                BYTES32_ZERO,
                signature,
                salt,
                SIGNATURE_TYPE);

        String polyOrderType = switch (orderType) {
            case FOK -> "FOK";
            case FAK -> "FAK";
        };

        return new SendOrder(clobOrder, credentials.apiKey(), polyOrderType, false, false);
    }
}

