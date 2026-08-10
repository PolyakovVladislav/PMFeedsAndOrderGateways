package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.ws.model;

public record PredictFunMarketEvent(
        long marketId,
        PredictFunOrderBook orderBook,
        long sourceTimestampMs,
        long receivedAtMs
) {
}

