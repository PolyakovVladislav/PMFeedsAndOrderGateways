package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderStateResponse(
        @JsonProperty("id") String id,
        @JsonProperty("status") String status,
        @JsonProperty("size_matched") String sizeMatched,
        @JsonProperty("original_size") String originalSize
) {

    public double sizeMatchedOrZero() {
        if (sizeMatched == null || sizeMatched.isBlank()) {
            return 0.0;
        }
        try {
            return Double.parseDouble(sizeMatched);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}

