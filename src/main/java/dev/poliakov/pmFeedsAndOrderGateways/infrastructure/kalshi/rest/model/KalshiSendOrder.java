package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record KalshiSendOrder(
        String ticker,
        @JsonProperty("client_order_id") String clientOrderId,
        String side,
        String count,
        String price,
        @JsonProperty("time_in_force") String timeInForce,
        @JsonProperty("self_trade_prevention_type") String selfTradePreventionType
) {
}

