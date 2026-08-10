package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest.model;

import java.util.List;

public record KalshiBatchOrdersRequest(List<KalshiSendOrder> orders) {
}

