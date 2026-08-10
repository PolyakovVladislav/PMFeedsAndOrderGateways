package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.ws.model;

import java.util.Map;

public record MarketEvent(
        String assetId,
        Map<String, OrderBook> orderBooks,
        Event event,
        long receivedAtMs
) {

    public sealed interface Event
            permits BookSnapshot, PriceChange, BestBidAsk, MarketResolved {
    }

    public record BookSnapshot(
            long sourceTimestamp
    ) implements Event {
    }

    public record PriceChange(
            long sourceTimestamp
    ) implements Event {
    }

    public record BestBidAsk(
            double bestBid,
            double bestAsk,
            long sourceTimestamp
    ) implements Event {
    }

    public record MarketResolved(
            String winningAssetId,
            String winningOutcome,
            long sourceTimestamp
    ) implements Event {
    }
}

