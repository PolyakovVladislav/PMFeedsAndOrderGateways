package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws.model;

// orderBook is an immutable snapshot rather than the live book: subscribers are handed a value they
// can read at leisure while deltas keep landing on the real one.
public record KalshiMarketEvent(
        String marketTicker,
        KalshiBookSnapshot orderBook,
        Event event,
        long receivedAtMs
) {

    public sealed interface Event permits Snapshot, Delta {
    }

    public record Snapshot(long sourceTimestampMs) implements Event {
    }

    public record Delta(String side, double priceDollars, double deltaFp, long sourceTimestampMs)
            implements Event {
    }
}

