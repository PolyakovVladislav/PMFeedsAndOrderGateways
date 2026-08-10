package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PredictFunSignedOrder(
        @JsonProperty("salt") String salt,
        @JsonProperty("maker") String maker,
        @JsonProperty("signer") String signer,
        @JsonProperty("taker") String taker,
        @JsonProperty("tokenId") String tokenId,
        @JsonProperty("makerAmount") String makerAmount,
        @JsonProperty("takerAmount") String takerAmount,
        @JsonProperty("expiration") String expiration,
        @JsonProperty("nonce") String nonce,
        @JsonProperty("feeRateBps") String feeRateBps,
        @JsonProperty("side") int side,
        @JsonProperty("signatureType") int signatureType,
        @JsonProperty("signature") String signature,
        @JsonProperty("hash") String hash
) {
}

