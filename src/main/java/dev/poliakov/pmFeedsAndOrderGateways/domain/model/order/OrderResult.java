package dev.poliakov.pmFeedsAndOrderGateways.domain.model.order;

/**
 * What a venue answered for one order.
 *
 * <p>{@code filledSize} is only as final as {@code status} allows — see
 * {@link Status#DELAYED}, where zero means "not yet", not "never".
 *
 * @param orderId    the venue's id; may be blank when the order was rejected outright
 * @param status     how far the order got
 * @param filledSize contracts filled by the time the venue answered
 */
public record OrderResult(
        String orderId,
        Status status,
        double filledSize
) {
    /**
     * How far an order got, normalised across venues.
     */
    public enum Status {
        /**
         * Resting on the book, unmatched so far.
         */
        LIVE,
        /**
         * Matched; {@code filledSize} is final.
         */
        MATCHED,
        /**
         * Accepted and marketable, but not matched yet. {@code filledSize} is zero for a reason
         * that says nothing about the outcome — resolve it with
         * {@link dev.poliakov.pmFeedsAndOrderGateways.domain.port.out.OrderGateway#resolveFilledSize}
         * rather than reading it as a miss.
         */
        DELAYED,
        /**
         * Refused by the venue; nothing was filled.
         */
        REJECTED
    }
}
