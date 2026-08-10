package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SendOrder(
        ClobOrder order,
        String owner,
        @JsonProperty("orderType") String orderType,
        @JsonProperty("postOnly") boolean postOnly,
        @JsonProperty("deferExec") boolean deferExec
) {
}

