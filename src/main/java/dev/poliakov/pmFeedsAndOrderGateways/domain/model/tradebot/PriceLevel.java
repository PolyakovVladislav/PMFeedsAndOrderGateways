package dev.poliakov.pmFeedsAndOrderGateways.domain.model.tradebot;

/**
 * One rung of an order book.
 *
 * @param price 0..1, in the leg's own terms — a NO leg's levels are already mirrored
 * @param size  contracts available at that price
 */
public record PriceLevel(double price, double size) {
}
