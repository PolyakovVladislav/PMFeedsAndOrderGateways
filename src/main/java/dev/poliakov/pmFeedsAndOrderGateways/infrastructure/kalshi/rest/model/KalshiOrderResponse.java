package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record KalshiOrderResponse(
        @JsonProperty("order_id") String orderId,
        @JsonProperty("client_order_id") String clientOrderId,
        @JsonProperty("fill_count") String fillCount,
        @JsonProperty("remaining_count") String remainingCount,
        @JsonProperty("average_fill_price") String averageFillPrice,
        @JsonProperty("average_fee_paid") String averageFeePaid,
        @JsonProperty("ts_ms") Long tsMs,
        KalshiApiError error
) {
}

