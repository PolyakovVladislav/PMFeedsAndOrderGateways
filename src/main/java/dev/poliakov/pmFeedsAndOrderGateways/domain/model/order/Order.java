package dev.poliakov.pmFeedsAndOrderGateways.domain.model.order;

/**
 * One order on one leg, in venue-neutral terms.
 *
 * <p>{@code price} and {@code bookPrice} are separate on purpose, and the difference matters on
 * Polymarket: a buy there is denominated in the cash it spends, so the cash budget is computed from
 * {@code bookPrice} — what the book actually showed — while {@code price} is only the limit the
 * order may not cross. Budgeting at the limit instead silently over-buys by {@code limit/fill}.
 *
 * @param token     which leg, and by extension which venue
 * @param side      buy or sell
 * @param price     limit price, 0..1; for a market order, the edge of the scale
 * @param size      contracts
 * @param bookPrice the price observed on the book when this order was decided, used for sizing
 */
public record Order(
        Token token,
        Side side,
        double price,
        double size,
        double bookPrice
) {
    /**
     * Direction of the order.
     */
    public enum Side {BUY, SELL}
}
