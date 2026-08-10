package dev.poliakov.pmFeedsAndOrderGateways.client;

import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Token;
import dev.poliakov.pmFeedsAndOrderGateways.domain.port.out.OrderBookFeed;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

// The builder wires the graph correctly and offline. None of these touch the network: credential
// derivation and socket opening are both lazy, so constructing the client only assembles objects.
class PredictionMarketClientTest {

    // The standard secp256k1 test key — a valid key web3j's signer accepts at construction.
    private static final String PM_KEY =
            "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    private static final String PM_ADDRESS = "0x2B5AD5c4795c026514f8317c7a215E218DcCD6cF";
    private static final String EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
    private static final String NEG_RISK_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a";

    @Test
    void buildsAPolymarketOnlyClientFromKeyAndAddressAlone() {
        // The two-argument form: the caller gives only what is theirs, and the built-in exchange
        // contracts are enough to construct the signer. No network is touched.
        try (PredictionMarketClient client = PredictionMarketClient.builder()
                .polymarket(PM_KEY, PM_ADDRESS)
                .build()) {
            assertNotNull(client.orderBookFeed(), "a configured client must expose a feed");
            assertNotNull(client.orderGateway(), "a configured client must expose a gateway");
        }
    }

    @Test
    void buildsAPredictFunClientFromApiKeyAndSigningKeyAlone() {
        // PredictFun derives its trading address from the key and defaults its contracts, so these
        // two inputs are the whole of it.
        try (PredictionMarketClient client = PredictionMarketClient.builder()
                .predictFun("pf-api-key", PM_KEY)
                .build()) {
            assertNotNull(client.orderBookFeed());
            assertNotNull(client.orderGateway());
        }
    }

    @Test
    void refusesToBuildWithNoVenueConfigured() {
        // An empty client would be a silent no-op that fails only later, at the first order. Fail now.
        assertThrows(IllegalStateException.class,
                () -> PredictionMarketClient.builder().build());
    }

    @Test
    void refusesToBuildWhenAVenueIsMissingARequiredCredential() {
        // A blank neg-risk contract would sign orders against nothing — reject it at build time, not
        // when a real order is on the line.
        assertThrows(IllegalArgumentException.class,
                () -> PredictionMarketClient.builder()
                        .polymarket(PM_KEY, PM_ADDRESS, EXCHANGE, "   ")
                        .build());
    }

    @Test
    void aLegForAnUnconfiguredVenueFailsLoudlyRatherThanStallingTheCombo() {
        try (PredictionMarketClient client = PredictionMarketClient.builder()
                .polymarket(PM_KEY, PM_ADDRESS, EXCHANGE, NEG_RISK_EXCHANGE)
                .build()) {
            OrderBookFeed feed = client.orderBookFeed();
            Token kalshiLeg = new Token.Kalshi("SOME-TICKER", Token.Outcome.YES);

            // Kalshi was never configured, so submitting a Kalshi leg must fail — not hang forever
            // waiting on a stream that will never emit.
            assertThrows(IllegalStateException.class,
                    () -> feed.combinedStream(List.of(kalshiLeg)));
        }
    }
}
