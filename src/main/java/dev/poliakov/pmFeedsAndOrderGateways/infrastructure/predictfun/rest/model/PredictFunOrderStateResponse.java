package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PredictFunOrderStateResponse(
        @JsonProperty("id") String id,
        @JsonProperty("status") String status,
        @JsonProperty("amount") String amount,
        @JsonProperty("amountFilled") String amountFilled,
        @JsonProperty("tokenId") String tokenId,
        @JsonProperty("marketId") Long marketId,
        @JsonProperty("side") Integer side,
        @JsonProperty("strategy") String strategy
) {

    public boolean terminal() {
        if (status == null) {
            return false;
        }
        return switch (status) {
            case "FILLED", "EXPIRED", "CANCELLED", "INVALIDATED" -> true;
            default -> false;
        };
    }
}

