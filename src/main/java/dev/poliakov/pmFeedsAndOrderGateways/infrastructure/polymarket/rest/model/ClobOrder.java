package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ClobOrder(
        String maker,
        String signer,
        @JsonProperty("tokenId") String tokenId,
        @JsonProperty("makerAmount") String makerAmount,
        @JsonProperty("takerAmount") String takerAmount,
        String side,
        String expiration,
        String timestamp,
        @JsonProperty("metadata") String metadata,
        String builder,
        String signature,
        int salt,
        @JsonProperty("signatureType") int signatureType
) {
}

