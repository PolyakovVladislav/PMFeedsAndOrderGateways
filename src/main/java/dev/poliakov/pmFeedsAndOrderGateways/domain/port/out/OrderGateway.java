package dev.poliakov.pmFeedsAndOrderGateways.domain.port.out;

import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Order;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.OrderResult;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.OrderType;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Token;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Places and cancels orders, in venue-neutral terms.
 *
 * <p>Obtain one from {@code PredictionMarketClient.orderGateway()}. That implementation routes by
 * {@link dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Exchange}, taken from each order's token,
 * so a caller never picks a venue explicitly — the token decides.
 *
 * <p><strong>Nothing here is atomic across venues.</strong> A multi-leg basket spanning two
 * exchanges is sent as one bulk request per exchange, concurrently, and either can fill, partially
 * fill or reject independently. Always read the returned {@link OrderResult} per leg and reconcile;
 * an unhedged leg is a real position, not an error to retry blindly.
 */
public interface OrderGateway {

    /**
     * Places a single order.
     *
     * @param order     the leg to place
     * @param orderType fill-or-kill or fill-and-kill
     * @return the venue's answer for this order
     */
    Mono<OrderResult> placeOrder(Order order, OrderType orderType);

    /**
     * Places several legs as one basket, one bulk request per venue.
     *
     * <p>The returned list is positional: element {@code i} is the outcome of {@code orders.get(i)},
     * and a leg the venue never answered for comes back {@link OrderResult.Status#REJECTED} with
     * zero filled rather than being omitted.
     *
     * @param orders    the legs, which may span venues
     * @param orderType fill-or-kill or fill-and-kill
     * @return one result per input leg, in input order
     */
    Mono<List<OrderResult>> placeOrders(List<Order> orders, OrderType orderType);

    /**
     * What an order actually filled, asked of the venue rather than inferred.
     *
     * <p>Exists for the case where the placement response cannot yet say: Polymarket answers
     * {@link OrderResult.Status#DELAYED} for an order it has accepted but not matched, whose
     * reported size is zero for a reason unrelated to the eventual outcome. Poll this to find out.
     *
     * @param token   the leg the order belongs to
     * @param orderId the venue's order id
     * @return the filled size, empty if the venue does not support the query, or an error if the
     * venue never acknowledged the order — which is not the same as a zero fill and must
     * not be recorded as one
     */
    default Mono<Double> resolveFilledSize(Token token, String orderId) {
        return Mono.empty();
    }

    /**
     * Cancels a resting order. Best effort: order ids carry no venue tag, so routing is by id shape.
     *
     * @param orderId the venue's order id
     * @return completion, or an error if the id cannot be attributed to a venue
     */
    Mono<Void> cancel(String orderId);

    /**
     * Warms up whatever is expensive on the first order — credential derivation, connection setup —
     * so that cost does not land on the first real trade. Optional but worth calling at startup.
     *
     * @return completion once every configured venue is ready
     */
    Mono<Void> warmup();

    /**
     * Pre-resolves any per-token venue metadata these legs will need, so the first order on them is
     * not delayed by a lookup.
     *
     * @param tokens legs expected to trade soon
     * @return completion once the metadata is cached
     */
    default Mono<Void> prepare(List<Token> tokens) {
        return Mono.empty();
    }
}
