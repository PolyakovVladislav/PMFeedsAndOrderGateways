package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.gateway;

import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Order;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.OrderResult;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.OrderType;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Token;
import dev.poliakov.pmFeedsAndOrderGateways.domain.port.out.OrderGateway;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.PredictFunCredentials;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.PredictFunApi;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.PredictFunMarketRegistry;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.model.PredictFunCreateOrderRequest;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.model.PredictFunCreateOrderResponse;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.model.PredictFunMarketInfo;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.model.PredictFunSignedOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class PredictFunOrderGateway implements OrderGateway {

    private static final Logger log = LoggerFactory.getLogger(PredictFunOrderGateway.class);

    private static final BigDecimal WEI = BigDecimal.TEN.pow(18);

    private static final int MAX_SALT = Integer.MAX_VALUE;
    private static final String ZERO_ADDRESS = "0x0000000000000000000000000000000000000000";

    private static final int SIGNATURE_TYPE_EOA = 0;

    private static final long MARKET_ORDER_TTL_SECONDS = 300;
    private static final int RESOLVE_POLLS = 8;
    private static final java.time.Duration RESOLVE_POLL_GAP = java.time.Duration.ofSeconds(1);
    private final PredictFunApi api;
    private final PredictFunCredentials credentials;
    private final PredictFunMarketRegistry markets;
    private final PredictFunEip712OrderSigner signer;

    public PredictFunOrderGateway(PredictFunApi api, PredictFunCredentials credentials,
                                  PredictFunMarketRegistry markets) {
        this.api = api;
        this.credentials = credentials;
        this.markets = markets;
        this.signer = new PredictFunEip712OrderSigner(credentials.privateKey(), credentials.chainId());
    }

    private static BigInteger[] toAmounts(Order.Side side, double size, double price, double bookPrice) {
        if (side == Order.Side.BUY) {
            BigInteger budget = toWei(bookPrice * size);

            BigInteger minShares = price > 0
                    ? toWei(bookPrice * size / price)
                    : toWei(size);
            return new BigInteger[]{budget, minShares};
        }
        BigInteger shares = toWei(size);
        BigInteger minCollateral = toWei(price * size);
        return new BigInteger[]{shares, minCollateral};
    }

    private static BigInteger toWei(double value) {
        return BigDecimal.valueOf(value).multiply(WEI).setScale(0, RoundingMode.DOWN).toBigInteger();
    }

    private static double fromWei(String weiValue) {
        if (weiValue == null || weiValue.isBlank()) {
            return 0.0;
        }
        try {
            return new BigDecimal(weiValue).divide(WEI, 8, RoundingMode.DOWN).doubleValue();
        } catch (NumberFormatException e) {
            log.warn("PredictFun returned an unparseable wei amount '{}'", weiValue);
            return 0.0;
        }
    }

    private static OrderResult toResult(PredictFunCreateOrderResponse response) {
        String orderId = response.orderId();
        if (!response.success() || orderId == null) {
            return new OrderResult(orderId, OrderResult.Status.REJECTED, 0.0);
        }
        return new OrderResult(orderId, OrderResult.Status.DELAYED, 0.0);
    }

    @Override
    public Mono<OrderResult> placeOrder(Order order, OrderType orderType) {
        if (!(order.token() instanceof Token.PredictFun token)) {
            return Mono.error(new IllegalArgumentException(
                    "PredictFunOrderGateway received a non-PredictFun leg: " + order.token()));
        }
        return markets.resolve(List.of(token.marketId()))
                .map(resolved -> buildRequest(order, token, resolved.get(token.marketId())))
                .flatMap(api::createOrder)
                .map(PredictFunOrderGateway::toResult);
    }

    @Override
    public Mono<Void> prepare(List<Token> tokens) {
        List<Long> marketIds = tokens.stream()
                .filter(Token.PredictFun.class::isInstance)
                .map(token -> ((Token.PredictFun) token).marketId())
                .distinct()
                .toList();
        return markets.prefetch(marketIds);
    }

    @Override
    public Mono<List<OrderResult>> placeOrders(List<Order> orders, OrderType orderType) {
        if (orders.isEmpty()) {
            return Mono.just(List.of());
        }
        return Flux.fromIterable(orders)
                .concatMap(order -> placeOrder(order, orderType)
                        .onErrorResume(err -> {
                            log.error("PredictFun leg {} failed outright — reporting it unfilled so the "
                                            + "caller can see the imbalance rather than losing the batch",
                                    order.token().label(), err);
                            return Mono.just(new OrderResult(null, OrderResult.Status.REJECTED, 0.0));
                        }))
                .collectList();
    }

    @Override
    public Mono<Double> resolveFilledSize(Token token, String orderId) {
        if (!(token instanceof Token.PredictFun) || orderId == null || orderId.isBlank()) {
            return Mono.empty();
        }
        return Mono.defer(() -> api.getOrders()
                        .map(orders -> orders.stream()
                                .filter(o -> orderId.equals(o.id()))
                                .findFirst()
                                .orElse(null)))
                .flatMap(state -> {
                    if (state == null) {

                        log.warn("PredictFun order {} not found when resolving its fill", orderId);
                        return Mono.empty();
                    }
                    double filled = fromWei(state.amountFilled());
                    log.info("PredictFun order {} state poll: status={} amountFilled={}",
                            orderId, state.status(), filled);
                    return state.terminal() ? Mono.just(filled) : Mono.empty();
                })
                .repeatWhenEmpty(RESOLVE_POLLS, attempts -> attempts.delayElements(RESOLVE_POLL_GAP))
                .doOnError(err -> log.error("PredictFun order {} could not be resolved — the recorded "
                        + "position for this leg is still the assumed size", orderId, err));
    }

    @Override
    public Mono<Void> cancel(String orderId) {
        return Mono.error(new UnsupportedOperationException(
                "PredictFun order cancellation requires an on-chain transaction to the CTF Exchange "
                        + "contract, which this bot cannot send. Cancel order " + orderId + " manually."));
    }

    @Override
    public Mono<Void> warmup() {
        return api.warmup();
    }

    private PredictFunCreateOrderRequest buildRequest(Order order, Token.PredictFun token,
                                                      PredictFunMarketInfo market) {
        int side = order.side() == Order.Side.BUY ? 0 : 1;
        BigInteger salt = BigInteger.valueOf(ThreadLocalRandom.current().nextInt(1, MAX_SALT));
        long expiration = System.currentTimeMillis() / 1000L + MARKET_ORDER_TTL_SECONDS;

        double venuePrice = token.outcome() == Token.Outcome.YES
                ? order.price()
                : 1.0 - order.price();
        double venueBookPrice = token.outcome() == Token.Outcome.YES
                ? order.bookPrice()
                : 1.0 - order.bookPrice();

        BigInteger[] amounts = toAmounts(order.side(), order.size(), venuePrice, venueBookPrice);

        String outcomeTokenId = market.tokenId(token.outcome());
        int feeRateBps = market.feeRateBps();
        String verifyingContract =
                credentials.exchangeContract(market.negRisk(), market.yieldBearing());

        String signature = signer.sign(
                verifyingContract,
                salt,
                credentials.address(),
                credentials.address(),
                ZERO_ADDRESS,
                outcomeTokenId,
                amounts[0],
                amounts[1],
                expiration,
                BigInteger.ZERO,
                feeRateBps,
                side,
                SIGNATURE_TYPE_EOA);

        PredictFunSignedOrder signed = new PredictFunSignedOrder(
                salt.toString(),
                credentials.address(),
                credentials.address(),
                ZERO_ADDRESS,
                outcomeTokenId,
                amounts[0].toString(),
                amounts[1].toString(),
                String.valueOf(expiration),
                "0",
                String.valueOf(feeRateBps),
                side,
                SIGNATURE_TYPE_EOA,
                signature,
                null);

        return PredictFunCreateOrderRequest.of(signed, toWei(venuePrice).toString(), "MARKET");
    }
}

