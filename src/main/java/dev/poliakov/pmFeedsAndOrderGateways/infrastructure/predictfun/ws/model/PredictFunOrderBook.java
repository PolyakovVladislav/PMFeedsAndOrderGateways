package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.ws.model;

import java.util.Comparator;
import java.util.List;

public record PredictFunOrderBook(
        long marketId,
        List<Level> yesBids,
        List<Level> yesAsks
) {

    private static final Comparator<Level> BEST_BID_FIRST =
            Comparator.comparingDouble(Level::price).reversed();
    private static final Comparator<Level> BEST_ASK_FIRST =
            Comparator.comparingDouble(Level::price);
    public PredictFunOrderBook {
        yesBids = yesBids.stream().sorted(BEST_BID_FIRST).toList();
        yesAsks = yesAsks.stream().sorted(BEST_ASK_FIRST).toList();
    }

    public boolean hasBothSides() {
        return !yesBids.isEmpty() && !yesAsks.isEmpty();
    }

    // Carries a price on at least one side; a one-sided book is tradeable in the direction that side
    // supports, so it is emitted rather than withheld. Only a book with no levels at all is dropped.
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
        return "PredictFunOrderBook[" + marketId + " yesBid=" + bestYesBid()
                + " yesAsk=" + bestYesAsk()
                + " levels=" + yesBids.size() + "/" + yesAsks.size() + "]";
    }

    public record Level(double price, double size) {
    }
}

