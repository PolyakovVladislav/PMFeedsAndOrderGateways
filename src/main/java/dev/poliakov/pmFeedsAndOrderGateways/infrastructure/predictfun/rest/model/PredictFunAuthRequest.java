package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PredictFunAuthRequest(
        @JsonProperty("signer") String signer,
        @JsonProperty("message") String message,
        @JsonProperty("signature") String signature
) {
}

