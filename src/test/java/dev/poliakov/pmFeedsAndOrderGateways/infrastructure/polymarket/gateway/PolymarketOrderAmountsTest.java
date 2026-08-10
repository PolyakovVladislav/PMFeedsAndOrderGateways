package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.gateway;

import com.fasterxml.jackson.databind.json.JsonMapper;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Order;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.OrderType;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Token;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.PolymarketClobApi;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.PolymarketCredentials;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.PolymarketNegRiskRegistry;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.model.ClobOrder;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.model.OrderResponse;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.model.SendOrder;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// A Polymarket buy is denominated in the cash it spends, not the tokens it receives: makerAmount is
// a budget and the exchange returns whatever it buys at the real fill price. Budgeting that cash at
// the limit rather than at the observed book price silently over-buys by limit/fill — live that
// reached +47% on a cheap leg, and every extra token was unhedged. Now that orders are market orders
// the limit is pinned at the edge of the scale (1.0 buying, 0.0 selling), so that budget is the only
// thing bounding the spend at all. These tests pin the arithmetic.
class PolymarketOrderAmountsTest {

    // What TradeBotLifecycleService puts on every order it builds.
    private static final double MARKET_BUY = 1.0;
    private static final double MARKET_SELL = 0.0;

    private static final String PRIVATE_KEY =
            "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    private static final String ADDRESS = "0x2B5AD5c4795c026514f8317c7a215E218DcCD6cF";
    private static final String TOKEN_ID =
            "10547381015916960267379463101229159185405356924982461726471550099674011526491";

    private static final double USDC = 1_000_000d;

    private static ClobOrder buildBuy(double limit, double size, double bookPrice) {
        return capture(new Order(new Token.Polymarket(TOKEN_ID), Order.Side.BUY, limit, size, bookPrice));
    }

    private static ClobOrder buildSell(double limit, double size, double bookPrice) {
        return capture(new Order(new Token.Polymarket(TOKEN_ID), Order.Side.SELL, limit, size, bookPrice));
    }

    private static ClobOrder capture(Order order) {
        CapturingClobApi clob = new CapturingClobApi();
        PolymarketOrderGateway gateway = new PolymarketOrderGateway(clob, new StubCredentials(),
                new PolymarketNegRiskRegistry(clob));
        gateway.placeOrders(List.of(order), OrderType.FAK).block();
        assertEquals(1, clob.posted.size(), "expected exactly one bulk request");
        return clob.posted.get(0).get(0).order();
    }

    @Test
    void buyBudgetsCashAtTheBookPriceSoAQuotedFillReturnsExactlyTheRequestedSize() {
        // The book that priced the live incident: 0.47 for the 5 contracts wanted.
        ClobOrder order = buildBuy(MARKET_BUY, 5.0, 0.47);

        double budget = Double.parseDouble(order.makerAmount());
        double minTokens = Double.parseDouble(order.takerAmount());

        // Cash is budgeted at what the book showed — never at the limit, which is now 1.0 and would
        // hand the exchange a budget more than twice the size of the intended trade.
        assertEquals(0.47 * 5.0 * USDC, budget, 1.0);

        // The property this exists for, and the one thing bounding a market buy: filling at the
        // quoted price returns exactly `size`, and the exchange cannot spend past the budget however
        // far the book has moved since the snapshot.
        assertEquals(5.0, budget / USDC / 0.47, 1e-6);

        // The limit lives in the ratio, so the market price shows up as a 1:1 cash-to-token floor.
        assertEquals(MARKET_BUY, budget / minTokens, 1e-9);

        // And the worst case is a shortfall, never an overshoot — which recovery knows how to chase.
        assertTrue(minTokens < 5.0 * USDC, "a limit above the book must lower the minimum accepted");
    }

    @Test
    void buyTakesTheMarketPriceAtFaceValueInsteadOfFallingBackToTheBook() {
        // The old code read `price > 0 ? price : bookPrice` and, failing that, derived the token floor
        // from size alone. Both were the gateway deciding what a price meant. At the market price the
        // floor is the budget itself — emphatically not the size-derived 10.0 tokens the old fallback
        // would have produced.
        ClobOrder order = buildBuy(MARKET_BUY, 10.0, 0.5);

        double budget = Double.parseDouble(order.makerAmount());
        double minTokens = Double.parseDouble(order.takerAmount());

        assertEquals(0.5 * 10.0 * USDC, budget, 1.0);
        assertEquals(budget, minTokens, 1.0);
        assertTrue(minTokens < 10.0 * USDC,
                "the token floor must come from the limit, not from the requested size");
    }

    @Test
    void sellHandsOverExactlyTheRequestedSizeAndAcceptsWhateverTheMarketPays() {
        // A sell's makerAmount is the tokens themselves, so quantity is already exact and the limit
        // only sets the minimum cash accepted back. At the market price the raw minimum would be
        // nil, but the gateway floors it at $0.01 (USDC_PRECISION_UNIT = 10_000 atomic) to avoid
        // a zero takerAmount that Polymarket may reject.
        ClobOrder order = buildSell(MARKET_SELL, 5.0, 0.31);

        assertEquals(5.0 * USDC, Double.parseDouble(order.makerAmount()), 1.0);
        assertEquals(10_000.0, Double.parseDouble(order.takerAmount()), 1e-9);
    }

    private static final class StubCredentials implements PolymarketCredentials {
        @Override
        public String apiKey() {
            return "test-api-key";
        }

        @Override
        public String address() {
            return ADDRESS;
        }

        @Override
        public String passphrase() {
            return "test-passphrase";
        }

        @Override
        public String apiSecret() {
            return "dGVzdC1zZWNyZXQ=";
        }

        @Override
        public String privateKey() {
            return PRIVATE_KEY;
        }

        @Override
        public String exchangeContract() {
            return "0xE111180000d2663C0091e4f400237545B87B996B";
        }

        @Override
        public String negRiskExchangeContract() {
            return "0xe2222d279d744050d28e00520010520000310F59";
        }
    }

    private static final class CapturingClobApi extends PolymarketClobApi {

        private final List<List<SendOrder>> posted = new ArrayList<>();

        private CapturingClobApi() {
            super(HttpClient.create(), JsonMapper.builder().build(), new StubCredentials());
        }

        @Override
        public Mono<Boolean> fetchNegRisk(String tokenId) {
            return Mono.just(false);
        }

        @Override
        public Mono<List<OrderResponse>> postOrders(List<SendOrder> sendOrders) {
            posted.add(List.copyOf(sendOrders));
            return Mono.just(sendOrders.stream()
                    .map(o -> new OrderResponse(true, "0xorder", "matched", "0", "0",
                            List.of(), List.of(), ""))
                    .toList());
        }
    }
}
