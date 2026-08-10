package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.gateway;

import com.fasterxml.jackson.databind.json.JsonMapper;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Order;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.OrderType;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Token;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.PolymarketClobApi;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.PolymarketCredentials;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.PolymarketNegRiskRegistry;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.model.OrderResponse;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.model.SendOrder;
import org.junit.jupiter.api.Test;
import org.web3j.utils.Numeric;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

// Signing an order against the wrong exchange contract is not a loud failure — it produces a
// perfectly valid signature for the wrong EIP-712 domain, which Polymarket rejects as an invalid
// signature while the other leg of a cross-exchange combo fills, leaving a naked position. So the
// thing worth pinning down is that the contract actually baked into the signature follows the
// market's neg-risk flag.
class PolymarketNegRiskSigningTest {

    // Throwaway key — signing is deterministic and offline, nothing here touches a real account.
    private static final String PRIVATE_KEY =
            "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    private static final String ADDRESS = "0x2B5AD5c4795c026514f8317c7a215E218DcCD6cF";

    private static final String CTF_EXCHANGE = "0xE111180000d2663C0091e4f400237545B87B996B";
    private static final String NEG_RISK_CTF_EXCHANGE = "0xe2222d279d744050d28e00520010520000310F59";

    private static final String NEG_RISK_TOKEN = "10547381015916960267379463101229159185405356924982461726471550099674011526491";
    private static final String BINARY_TOKEN = "114412653756823633577819915585136836249665904236940177922695635921203289079836";

    // The Solady nested signature lays out 65 bytes of ECDSA, then the app domain separator. That
    // separator is derived from the verifying contract, so it is where "which exchange" ends up.
    private static byte[] domainSeparatorOf(String signatureHex) {
        byte[] blob = Numeric.hexStringToByteArray(signatureHex);
        byte[] separator = new byte[32];
        System.arraycopy(blob, 65, separator, 0, 32);
        return separator;
    }

    private static byte[] expectedSeparator(String contract) {
        // Sign a throwaway order with a signer built directly for this contract and read back the
        // separator it produced, rather than restating the domain-hash construction here.
        String signature = new Eip712OrderSigner(PRIVATE_KEY, contract)
                .sign(1, ADDRESS, ADDRESS, "1", java.math.BigInteger.ONE, java.math.BigInteger.ONE,
                        1L, 0, 3);
        return domainSeparatorOf(signature);
    }

    private static void assertArrayEqualsTo(byte[] expected, byte[] actual, String message) {
        assertEquals(Numeric.toHexString(expected), Numeric.toHexString(actual), message);
    }

    private static Order buy(String assetId) {
        return new Order(new Token.Polymarket(assetId), Order.Side.BUY, 0.5, 10, 0.5);
    }

    private static PolymarketOrderGateway gatewayFor(CapturingClobApi clob) {
        return new PolymarketOrderGateway(clob, new StubCredentials(),
                new PolymarketNegRiskRegistry(clob));
    }

    @Test
    void signsEachLegAgainstTheContractItsMarketSettlesOn() {
        CapturingClobApi clob = new CapturingClobApi(Map.of(
                NEG_RISK_TOKEN, true,
                BINARY_TOKEN, false));
        PolymarketOrderGateway gateway = gatewayFor(clob);

        gateway.placeOrders(List.of(
                        buy(NEG_RISK_TOKEN),
                        buy(BINARY_TOKEN)),
                OrderType.FAK).block();

        assertEquals(1, clob.posted.size(), "both legs should go out in one bulk request");
        List<SendOrder> sent = clob.posted.get(0);
        assertEquals(2, sent.size());

        byte[] negRiskLeg = domainSeparatorOf(sent.get(0).order().signature());
        byte[] binaryLeg = domainSeparatorOf(sent.get(1).order().signature());

        assertArrayEqualsTo(expectedSeparator(NEG_RISK_CTF_EXCHANGE), negRiskLeg,
                "neg-risk market must be signed against the Neg Risk CTF Exchange");
        assertArrayEqualsTo(expectedSeparator(CTF_EXCHANGE), binaryLeg,
                "binary market must be signed against the plain CTF Exchange");
        assertNotEquals(Numeric.toHexString(negRiskLeg), Numeric.toHexString(binaryLeg));
    }

    @Test
    void refusesToSignWhenTheMarketTypeCannotBeResolved() {
        // A token the CLOB has no book for: the old code would have signed it against the default
        // contract and discovered the mistake only from a rejected order.
        CapturingClobApi clob = new CapturingClobApi(Map.of());
        PolymarketOrderGateway gateway = gatewayFor(clob);

        assertThrows(Exception.class,
                () -> gateway.placeOrders(List.of(buy(NEG_RISK_TOKEN)), OrderType.FAK).block());
        assertTrue(clob.posted.isEmpty(), "nothing may reach the exchange when the contract is unknown");
    }

    @Test
    void looksUpEachTokenOnlyOnce() {
        CapturingClobApi clob = new CapturingClobApi(Map.of(BINARY_TOKEN, false));
        PolymarketOrderGateway gateway = gatewayFor(clob);

        gateway.placeOrders(List.of(buy(BINARY_TOKEN)), OrderType.FAK).block();
        gateway.placeOrders(List.of(buy(BINARY_TOKEN)), OrderType.FAK).block();

        assertEquals(1, clob.lookups.get(),
                "the flag is immutable per market, so it must be cached after the first lookup");
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
            return CTF_EXCHANGE;
        }

        @Override
        public String negRiskExchangeContract() {
            return NEG_RISK_CTF_EXCHANGE;
        }
    }

    // Stands in for the live CLOB: answers neg-risk from a fixed table and records what was posted
    // instead of sending it.
    private static final class CapturingClobApi extends PolymarketClobApi {

        private final Map<String, Boolean> negRiskByToken;
        private final List<List<SendOrder>> posted = new ArrayList<>();
        private final AtomicInteger lookups = new AtomicInteger();

        private CapturingClobApi(Map<String, Boolean> negRiskByToken) {
            super(HttpClient.create(), JsonMapper.builder().build(), new StubCredentials());
            this.negRiskByToken = negRiskByToken;
        }

        @Override
        public Mono<Boolean> fetchNegRisk(String tokenId) {
            lookups.incrementAndGet();
            Boolean negRisk = negRiskByToken.get(tokenId);
            return negRisk == null
                    ? Mono.error(new IllegalStateException("no book for " + tokenId))
                    : Mono.just(negRisk);
        }

        @Override
        public Mono<List<OrderResponse>> postOrders(List<SendOrder> sendOrders) {
            posted.add(List.copyOf(sendOrders));
            return Mono.just(sendOrders.stream()
                    .map(o -> new OrderResponse(true, "0xorder", "matched", "5", "10",
                            List.of(), List.of(), ""))
                    .toList());
        }
    }
}
