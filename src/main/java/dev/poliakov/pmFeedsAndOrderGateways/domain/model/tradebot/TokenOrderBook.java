package dev.poliakov.pmFeedsAndOrderGateways.domain.model.tradebot;

import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Token;

import java.util.List;

/**
 * One leg's book, best-priced first: {@code bids} descending, {@code asks} ascending. Index 0 is
 * therefore the top of book on either side.
 *
 * <p>Either list can be empty — a one-sided book is normal for a deep out-of-the-money leg, so check
 * the side you need before reading index 0.
 *
 * @param token the leg this book belongs to
 * @param bids  what buyers are offering, highest first
 * @param asks  what sellers are asking, lowest first
 */
public record TokenOrderBook(Token token, List<PriceLevel> bids, List<PriceLevel> asks) {
}
