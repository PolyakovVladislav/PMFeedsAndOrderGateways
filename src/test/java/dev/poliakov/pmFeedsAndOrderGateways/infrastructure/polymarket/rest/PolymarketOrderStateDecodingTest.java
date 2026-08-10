package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;

// Decoding GET /data/order/{id}.
//
// The venue answers 200 with a literal `null` body while it is still indexing an order it has
// already accepted. Jackson decodes that to a Java null, and a null returned from Mono.map is
// illegal in Reactor: live this surfaced as
//   NullPointerException: The mapper [...PolymarketClobApi$$Lambda...] returned a null value
// which aborted the DELAYED poll loop on its first attempt and pushed real positions into manual
// reconciliation.
class PolymarketOrderStateDecodingTest {

    private static final ObjectMapper MAPPER = JsonMapper.builder().build();

    private static byte[] bytes(String body) {
        return body.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void aLiteralNullBodyDecodesToEmptyRatherThanThrowing() {
        // The exact byte sequence that broke production.
        StepVerifier.create(PolymarketClobApi.decodeOrderState(bytes("null"), MAPPER))
                .verifyComplete();
    }

    @Test
    void aRealStateDecodesToItsMatchedSize() {
        StepVerifier.create(PolymarketClobApi.decodeOrderState(
                        bytes("{\"id\":\"0xabc\",\"status\":\"matched\",\"size_matched\":\"6.12\"}"), MAPPER))
                .assertNext(state -> {
                    if (state.sizeMatchedOrZero() != 6.12) {
                        throw new AssertionError("expected 6.12, got " + state.sizeMatchedOrZero());
                    }
                })
                .verifyComplete();
    }

    @Test
    void aBodyThatIsNotJsonFailsLoudlyInsteadOfLookingLikeAnUnknownOrder() {
        // Empty and malformed bodies must stay errors. Folding them into the same empty as `null`
        // would let a broken response masquerade as "not indexed yet", and the caller would keep
        // polling a corpse instead of reporting a problem.
        StepVerifier.create(PolymarketClobApi.decodeOrderState(bytes(""), MAPPER))
                .verifyError(RuntimeException.class);
    }
}
