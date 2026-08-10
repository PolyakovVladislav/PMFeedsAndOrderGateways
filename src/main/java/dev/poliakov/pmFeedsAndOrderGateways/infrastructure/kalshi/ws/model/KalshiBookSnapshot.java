package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws.model;

import java.util.List;

// An immutable point-in-time view of one market's book, in yes-leg pricing, best-priced first:
// bids descending, asks ascending.
//
// This is what gets emitted to subscribers, rather than a copy of the live KalshiOrderBook. The book
// itself has to stay a mutable TreeMap because deltas are applied to it in place, but copying that
// TreeMap on every tick allocated a node per level for a structure the consumer immediately walked
// into lists anyway. Emitting the lists directly skips the middle entirely.
//
// The ordering is a guarantee of this type, not of whoever built it: everything downstream reads
// index 0 as the best price and walks outward, so a snapshot built any other way would silently
// price orders off the wrong end of the book.
public record KalshiBookSnapshot(
        String marketTicker,
        List<Level> yesBids,
        List<Level> yesAsks
) {

    public boolean hasBothSides() {
        return !yesBids.isEmpty() && !yesAsks.isEmpty();
    }

    // Whether this book is worth emitting at all — see OrderBook.hasAnySide for the reasoning.
    //
    // Worth spelling out for Kalshi specifically: one book serves both outcomes, and the NO leg is
    // its mirror. A yes-side book with asks and no bids gives a NO leg with bids and no asks, so the
    // same one-sided book is buyable as YES and sellable as NO. Blocking it denied both.
    public boolean hasAnySide() {
        return !yesBids.isEmpty() || !yesAsks.isEmpty();
    }

    public double bestYesBid() {
        return yesBids.isEmpty() ? 0 : yesBids.get(0).price();
    }

    public double bestYesAsk() {
        return yesAsks.isEmpty() ? 0 : yesAsks.get(0).price();
    }

    @Override
    public String toString() {
        return "KalshiBookSnapshot[" + marketTicker + " yesBid=" + bestYesBid()
                + " yesAsk=" + bestYesAsk()
                + " levels=" + yesBids.size() + "/" + yesAsks.size() + "]";
    }

    // price is in yes-leg terms; size is in contracts.
    public record Level(double price, double size) {
    }
}
