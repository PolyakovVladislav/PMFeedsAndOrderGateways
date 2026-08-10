package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.gateway;

import com.fasterxml.jackson.databind.json.JsonMapper;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Order;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.OrderType;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Token;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.KalshiCredentials;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest.KalshiTradeApi;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest.model.KalshiOrderResponse;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest.model.KalshiSendOrder;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Kalshi quotes one book off the yes leg, so a NO leg is flipped to `1 - p` on the way out. With
// market orders the domain price is now always near an edge of the scale, which sends that flip
// straight through the other edge — the mapping stays correct only because side and price move
// together, and getting it backwards would turn a market buy into an order that can never fill.
// This gateway does no clamping of its own — it takes order.price() at face value and just flips it
// — so what reaches the wire is exactly the domain's 0.99/0.01, not the mathematical 1.0/0.0 edge
// (both venues reject a price exactly there; see MARKET_BUY_PRICE in TradeBotLifecycleService).
class KalshiMarketOrderMappingTest {

    private static final String TICKER = "TEST-TICKER";

    // What TradeBotLifecycleService puts on every order it builds — MARKET_BUY_PRICE / MARKET_SELL_PRICE.
    private static final double MARKET_BUY = 0.99;
    private static final double MARKET_SELL = 0.01;

    private static KalshiSendOrder capture(Token.Outcome outcome, Order.Side side, double price) {
        CapturingTradeApi api = new CapturingTradeApi();
        KalshiOrderGateway gateway = new KalshiOrderGateway(api);
        // bookPrice is deliberately something the market price is not, so a mix-up would show.
        Order order = new Order(new Token.Kalshi(TICKER, outcome), side, price, 5.0, 0.37);
        gateway.placeOrders(List.of(order), OrderType.FAK).block();
        assertEquals(1, api.posted.size(), "expected exactly one batch");
        return api.posted.get(0).get(0);
    }

    @Test
    void yesBuyBidsAtTheTopOfTheScale() {
        KalshiSendOrder wire = capture(Token.Outcome.YES, Order.Side.BUY, MARKET_BUY);
        assertEquals("bid", wire.side());
        assertEquals("0.9900", wire.price());
    }

    @Test
    void yesSellAsksAtTheBottomOfTheScale() {
        KalshiSendOrder wire = capture(Token.Outcome.YES, Order.Side.SELL, MARKET_SELL);
        assertEquals("ask", wire.side());
        assertEquals("0.0100", wire.price());
    }

    @Test
    void noBuyAsksAtTheBottomOfTheScale() {
        // Buying NO at up to $1 is selling YES at $0 or better — the ask has to go to the floor, not
        // to the ceiling the domain price came in at.
        KalshiSendOrder wire = capture(Token.Outcome.NO, Order.Side.BUY, MARKET_BUY);
        assertEquals("ask", wire.side());
        assertEquals("0.0100", wire.price());
    }

    @Test
    void noSellBidsAtTheTopOfTheScale() {
        // Selling NO for anything at all is buying YES at up to $1.
        KalshiSendOrder wire = capture(Token.Outcome.NO, Order.Side.SELL, MARKET_SELL);
        assertEquals("bid", wire.side());
        assertEquals("0.9900", wire.price());
    }

    @Test
    void sizeIsStillRenderedAtContractScale() {
        KalshiSendOrder wire = capture(Token.Outcome.YES, Order.Side.BUY, MARKET_BUY);
        assertEquals("5.00", wire.count());
        assertEquals("immediate_or_cancel", wire.timeInForce());
        assertEquals(TICKER, wire.ticker());
    }

    private static final class CapturingTradeApi extends KalshiTradeApi {

        private final List<List<KalshiSendOrder>> posted = new ArrayList<>();

        private CapturingTradeApi() {
            // immediate() keeps the signing hop out of the way: production pushes RSA-PSS onto a
            // dedicated scheduler, but here the assertion is about what reaches the wire, and an
            // extra thread would only add nondeterminism to it.
            super(HttpClient.create(), JsonMapper.builder().build(), new StubCredentials(), null);
        }

        @Override
        public Mono<List<KalshiOrderResponse>> postOrders(List<KalshiSendOrder> orders) {
            posted.add(List.copyOf(orders));
            return Mono.just(orders.stream()
                    .map(o -> new KalshiOrderResponse("order-id", o.clientOrderId(), o.count(), "0",
                            "0.3700", "0.0100", 0L, null))
                    .toList());
        }
    }

    private static final class StubCredentials implements KalshiCredentials {
        @Override
        public String apiKeyId() {
            return "test-key-id";
        }

        @Override
        public String sign(String message) {
            return "test-signature";
        }
    }
}
