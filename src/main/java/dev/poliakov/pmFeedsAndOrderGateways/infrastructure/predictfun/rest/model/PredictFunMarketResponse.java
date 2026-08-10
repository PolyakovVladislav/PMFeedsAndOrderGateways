package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PredictFunMarketResponse(
        @JsonProperty("success") boolean success,
        @JsonProperty("data") Market data
) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Market(
            @JsonProperty("id") Long id,
            @JsonProperty("isNegRisk") Boolean isNegRisk,
            @JsonProperty("isYieldBearing") Boolean isYieldBearing,

            @JsonProperty("feeRateBps") Integer feeRateBps,
            @JsonProperty("tradingStatus") String tradingStatus,
            @JsonProperty("outcomes") List<Outcome> outcomes
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Outcome(
            @JsonProperty("name") String name,
            @JsonProperty("indexSet") Integer indexSet,
            @JsonProperty("onChainId") String onChainId
    ) {
    }
}

