package dev.poliakov.pmFeedsAndOrderGateways.domain.model.order;

/**
 * How much of an order the venue may leave unfilled.
 */
public enum OrderType {
    /**
     * Fill-or-kill: all of it at once, or nothing.
     */
    FOK,
    /**
     * Fill-and-kill: take whatever is available now, cancel the rest.
     */
    FAK
}
