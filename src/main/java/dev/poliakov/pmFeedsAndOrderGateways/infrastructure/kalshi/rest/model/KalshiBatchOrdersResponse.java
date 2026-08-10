package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record KalshiBatchOrdersResponse(List<KalshiOrderResponse> orders) {
}

