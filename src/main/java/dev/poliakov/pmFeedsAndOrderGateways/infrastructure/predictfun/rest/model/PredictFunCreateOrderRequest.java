package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PredictFunCreateOrderRequest(@JsonProperty("data") Data data) {

    public static PredictFunCreateOrderRequest of(PredictFunSignedOrder order, String pricePerShare,
                                                  String strategy) {
        return new PredictFunCreateOrderRequest(new Data(order, pricePerShare, strategy));
    }

    public record Data(
            @JsonProperty("order") PredictFunSignedOrder order,
            @JsonProperty("pricePerShare") String pricePerShare,
            @JsonProperty("strategy") String strategy
    ) {
    }
}

