package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.gateway;

import com.fasterxml.jackson.databind.json.JsonMapper;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Token;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.PolymarketClobApi;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.PolymarketCredentials;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.PolymarketNegRiskRegistry;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.model.OrderStateResponse;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

// Resolving what a DELAYED Polymarket order actually filled.
//
// The endpoint answers 200 with a literal `null` body while it is still indexing an order it has
// already accepted. That is "no information yet" — distinct from "matched nothing" — and the two
// have opposite consequences: reading a not-yet-indexed order as a zero fill writes the leg down to
// nothing, recovery buys it a second time, and a late match leaves double the intended position
// unhedged.
//
// Live, the null also arrived as a NullPointerException out of the API layer, which aborted the
// poll loop on its first attempt: a few hundred milliseconds of indexing lag became permanent
// manual reconciliation.
class PolymarketDelayedResolutionTest {

    private static final Token.Polymarket TOKEN = new Token.Polymarket(
            "10547381015916960267379463101229159185405356924982461726471550099674011526491");
    private static final String ORDER_ID = "0xorder";

    private static final String PRIVATE_KEY =
            "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    private static final String ADDRESS = "0x2B5AD5c4795c026514f8317c7a215E218DcCD6cF";

    private static OrderStateResponse state(String sizeMatched) {
        return new OrderStateResponse(ORDER_ID, "matched", sizeMatched, "10");
    }

    private static PolymarketOrderGateway gatewayWith(StubClobApi clob) {
        return new PolymarketOrderGateway(clob, new StubCredentials(),
                new PolymarketNegRiskRegistry(clob));
    }

    @Test
    void aTransientlyUnknownOrderIsPolledAgainRatherThanAbandoned() {
        // The regression itself. The first two polls find nothing indexed; the third reports the
        // real fill. Nothing here may give up early — the answer exists, it is just late.
        StubClobApi clob = new StubClobApi(poll -> poll < 2 ? null : state("7"));
        PolymarketOrderGateway gateway = gatewayWith(clob);

        StepVerifier.withVirtualTime(() -> gateway.resolveFilledSize(TOKEN, ORDER_ID))
                .thenAwait(Duration.ofSeconds(30))
                .expectNext(7.0)
                .verifyComplete();
    }

    @Test
    void anOrderTheVenueNeverAcknowledgesFailsInsteadOfReportingZeroFilled() {
        // The dangerous case, and the reason this cannot simply default to 0.0. Every poll comes
        // back unindexed, so the fill is genuinely unknown. Reporting zero would be a guess in the
        // one direction that loses money; failing keeps the assumed size and routes it to a human.
        StubClobApi clob = new StubClobApi(poll -> null);
        PolymarketOrderGateway gateway = gatewayWith(clob);

        StepVerifier.withVirtualTime(() -> gateway.resolveFilledSize(TOKEN, ORDER_ID))
                .thenAwait(Duration.ofSeconds(30))
                .expectError(IllegalStateException.class)
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void anOrderTheVenueKnowsAndReportsUnmatchedResolvesToZero() {
        // The other half of the distinction: here the venue does answer, and its answer is that
        // nothing matched. For a FAK order that is final, not a timeout, and must stay a plain 0.0
        // — otherwise every genuinely unfilled order would page a human.
        StubClobApi clob = new StubClobApi(poll -> state("0"));
        PolymarketOrderGateway gateway = gatewayWith(clob);

        StepVerifier.withVirtualTime(() -> gateway.resolveFilledSize(TOKEN, ORDER_ID))
                .thenAwait(Duration.ofSeconds(30))
                .expectNext(0.0)
                .verifyComplete();
    }

    // Answers getOrder from a per-poll function. A null return stands for the venue's literal `null`
    // body — the API layer turns that into an empty Mono, which is what the poll loop must survive.
    private static final class StubClobApi extends PolymarketClobApi {

        private final IntFunction<OrderStateResponse> answers;
        private final AtomicInteger polls = new AtomicInteger();

        private StubClobApi(IntFunction<OrderStateResponse> answers) {
            super(HttpClient.create(), JsonMapper.builder().build(), new StubCredentials());
            this.answers = answers;
        }

        @Override
        public Mono<OrderStateResponse> getOrder(String orderId) {
            return Mono.defer(() -> Mono.justOrEmpty(answers.apply(polls.getAndIncrement())));
        }
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
}
