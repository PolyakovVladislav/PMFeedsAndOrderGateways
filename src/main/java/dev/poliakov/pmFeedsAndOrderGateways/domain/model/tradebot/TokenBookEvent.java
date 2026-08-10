package dev.poliakov.pmFeedsAndOrderGateways.domain.model.tradebot;

import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Token;

import java.util.Map;

/**
 * One leg's book at one instant, as delivered by
 * {@link dev.poliakov.pmFeedsAndOrderGateways.domain.port.out.OrderBookFeed}.
 *
 * <p>{@code receivedAtMs} is when this leg last produced data, not when the batch was assembled.
 * Legs tick independently, so compare it against the clock before trading: on a quiet venue a leg
 * can be minutes old while its counterpart is fresh, and a stale book is the usual reason a
 * seemingly good price is not actually there.
 *
 * @param orderBooks   the book, keyed by leg
 * @param event        what caused this update
 * @param receivedAtMs local wall-clock time this leg's update arrived, epoch millis
 */
public record TokenBookEvent(
        Map<Token, TokenOrderBook> orderBooks,
        Event event,
        long receivedAtMs
) {

    /**
     * What produced an update.
     */
    public sealed interface Event permits BookSnapshot, PriceUpdate, MarketResolved {
    }

    /**
     * A full book replacement. {@code sourceTimestamp} is the venue's own clock, epoch millis.
     */
    public record BookSnapshot(long sourceTimestamp) implements Event {
    }

    /**
     * An incremental change already applied to the book.
     */
    public record PriceUpdate(long sourceTimestamp) implements Event {
    }

    /**
     * The market settled; it no longer trades and the winning outcome is known.
     */
    public record MarketResolved(
            String winningAssetId,
            String winningOutcome,
            long sourceTimestamp
    ) implements Event {
    }
}
