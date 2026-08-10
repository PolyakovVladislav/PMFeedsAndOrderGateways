package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PredictFunOrdersResponse(
        @JsonProperty("success") boolean success,
        @JsonProperty("data") List<PredictFunOrderStateResponse> data
) {
}

