package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record KalshiApiError(String code, String message, String details, String service) {
}

