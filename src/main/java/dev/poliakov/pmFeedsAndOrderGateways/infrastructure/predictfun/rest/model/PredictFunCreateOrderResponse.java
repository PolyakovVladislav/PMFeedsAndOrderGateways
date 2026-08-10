package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PredictFunCreateOrderResponse(
        @JsonProperty("success") boolean success,
        @JsonProperty("data") Data data
) {

    public String orderId() {
        return data == null ? null : data.orderId();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Data(
            @JsonProperty("code") String code,
            @JsonProperty("orderId") String orderId,
            @JsonProperty("orderHash") String orderHash
    ) {
    }
}

