package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.ws.model;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public final class OrderBook {

    public final String assetId;

    private final NavigableMap<Double, Double> bids = new TreeMap<>();
    private final NavigableMap<Double, Double> asks = new TreeMap<>();

    public OrderBook(String assetId) {
        this.assetId = assetId;
    }

    public OrderBook copy() {
        OrderBook copy = new OrderBook(assetId);
        copy.bids.putAll(bids);
        copy.asks.putAll(asks);
        return copy;
    }

    public void clear() {
        bids.clear();
        asks.clear();
    }

    public void applyLevel(boolean bid, double price, double size) {
        NavigableMap<Double, Double> side = bid ? bids : asks;
        if (size == 0.0) {
            side.remove(price);
        } else {
            side.put(price, size);
        }
    }

    public boolean hasBothSides() {
        return !bids.isEmpty() && !asks.isEmpty();
    }

    // Carries a price on at least one side. A one-sided book is a normal, tradeable shape — a deep
    // out-of-the-money leg quoting asks with nobody bidding — so it is emitted and each direction is
    // judged separately downstream. Only a book with no levels at all says nothing.
    public boolean hasAnySide() {
        return !bids.isEmpty() || !asks.isEmpty();
    }

    public double bestBid() {
        return bids.isEmpty() ? 0 : bids.lastKey();
    }

    public double bestBidSize() {
        return bids.isEmpty() ? 0 : bids.lastEntry().getValue();
    }

    public double bestAsk() {
        return asks.isEmpty() ? 0 : asks.firstKey();
    }

    public double bestAskSize() {
        return asks.isEmpty() ? 0 : asks.firstEntry().getValue();
    }

    public double sizeAt(boolean bid, double price) {
        return (bid ? bids : asks).getOrDefault(price, 0.0);
    }

    public List<Map.Entry<Double, Double>> levels(boolean bid) {
        NavigableMap<Double, Double> side = bid ? bids.descendingMap() : asks;
        return List.copyOf(side.entrySet());
    }

    @Override
    public String toString() {
        return "OrderBook[bid=" + bestBid() + "@" + bestBidSize()
                + ", ask=" + bestAsk() + "@" + bestAskSize()
                + ", levels=" + bids.size() + "/" + asks.size() + "]";
    }
}

