package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws.model;

import java.util.*;

public final class KalshiOrderBook {

    public final String marketTicker;

    private final NavigableMap<Double, Double> yesBids = new TreeMap<>();
    private final NavigableMap<Double, Double> yesAsks = new TreeMap<>();

    public KalshiOrderBook(String marketTicker) {
        this.marketTicker = marketTicker;
    }

    private static void put(NavigableMap<Double, Double> side, double price, double size) {
        if (size <= 0.0) {
            side.remove(price);
        } else {
            side.put(price, size);
        }
    }

    private static void add(NavigableMap<Double, Double> side, double price, double delta) {
        double updated = side.getOrDefault(price, 0.0) + delta;
        if (updated <= 1e-9) {
            side.remove(price);
        } else {
            side.put(price, updated);
        }
    }

    // An immutable, best-priced-first view for subscribers.
    //
    // This replaced a full copy() of both TreeMaps. That copy allocated a tree node per price level
    // on every tick, to hand over a structure the consumer immediately flattened into lists — so the
    // book was effectively materialised three times per tick (copy, entry-set copy, PriceLevel
    // list). Building the lists once here is both cheaper and closer to what anyone downstream
    // actually wants.
    //
    // Snapshots are taken per tick, so the lists are sized up front rather than grown.
    public KalshiBookSnapshot snapshot() {
        List<KalshiBookSnapshot.Level> bids = new ArrayList<>(yesBids.size());
        // Descending: the best bid is the highest.
        for (Map.Entry<Double, Double> level : yesBids.descendingMap().entrySet()) {
            bids.add(new KalshiBookSnapshot.Level(level.getKey(), level.getValue()));
        }
        List<KalshiBookSnapshot.Level> asks = new ArrayList<>(yesAsks.size());
        // Ascending: the best ask is the lowest, which is the map's natural order.
        for (Map.Entry<Double, Double> level : yesAsks.entrySet()) {
            asks.add(new KalshiBookSnapshot.Level(level.getKey(), level.getValue()));
        }
        return new KalshiBookSnapshot(marketTicker, bids, asks);
    }

    public void clear() {
        yesBids.clear();
        yesAsks.clear();
    }

    public void putYesBid(double price, double size) {
        put(yesBids, price, size);
    }

    public void putYesAsk(double price, double size) {
        put(yesAsks, price, size);
    }

    public void addYesBid(double price, double delta) {
        add(yesBids, price, delta);
    }

    public void addYesAsk(double price, double delta) {
        add(yesAsks, price, delta);
    }

    public boolean hasBothSides() {
        return !yesBids.isEmpty() && !yesAsks.isEmpty();
    }

    // Carries a price on at least one side. One Kalshi book serves both outcomes, so a yes-side with
    // asks but no bids is buyable as YES and sellable as NO — holding it back would deny both. Only a
    // book with no levels at all is withheld.
    public boolean hasAnySide() {
        return !yesBids.isEmpty() || !yesAsks.isEmpty();
    }

    public double bestYesBid() {
        return yesBids.isEmpty() ? 0 : yesBids.lastKey();
    }

    public double bestYesAsk() {
        return yesAsks.isEmpty() ? 0 : yesAsks.firstKey();
    }

    public List<Map.Entry<Double, Double>> yesBidLevels() {
        return List.copyOf(yesBids.descendingMap().entrySet());
    }

    public List<Map.Entry<Double, Double>> yesAskLevels() {
        return List.copyOf(yesAsks.entrySet());
    }

    @Override
    public String toString() {
        return "KalshiOrderBook[" + marketTicker + " yesBid=" + bestYesBid()
                + " yesAsk=" + bestYesAsk()
                + " levels=" + yesBids.size() + "/" + yesAsks.size() + "]";
    }
}

