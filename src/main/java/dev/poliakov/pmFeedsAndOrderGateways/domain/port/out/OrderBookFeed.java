package dev.poliakov.pmFeedsAndOrderGateways.domain.port.out;

import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Token;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.tradebot.TokenBookEvent;
import reactor.core.publisher.Flux;

import java.util.List;

/**
 * Live order books for a set of legs, normalised across venues into one shape.
 *
 * <p>Obtain one from {@code PredictionMarketClient.orderBookFeed()}. Each venue is subscribed on
 * its own WebSocket and every book is mapped to {@code bids}/{@code asks} in the leg's own terms,
 * so a Kalshi NO leg and a Polymarket leg read identically — no per-venue special cases in the
 * consumer.
 */
public interface OrderBookFeed {

    /**
     * Subscribes to every leg and emits whenever any of them ticks.
     *
     * <p>Each emission carries the latest book for <em>all</em> legs, not just the one that moved,
     * so a basket can be priced from a single element. Legs update independently, so their
     * timestamps differ: check {@link TokenBookEvent#receivedAtMs()} per leg before acting, since a
     * quiet venue can leave one leg minutes older than the rest.
     *
     * <p>A one-sided book is emitted rather than withheld — asks with no bids is the normal shape of
     * a deep out-of-the-money leg. <strong>The caller must check the side it needs:</strong> no asks
     * means nothing to buy, no bids means nothing to sell.
     *
     * <p>The socket opens on subscribe and closes when the last subscriber goes away. Submitting a
     * leg for a venue the client was not configured with throws {@link IllegalStateException}.
     *
     * @param tokens the legs to watch, across any mix of venues
     * @return an endless stream of book snapshots, one list element per input leg
     */
    Flux<List<TokenBookEvent>> combinedStream(List<Token> tokens);
}
