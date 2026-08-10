package dev.poliakov.pmFeedsAndOrderGateways.domain.model.order;

import dev.poliakov.pmFeedsAndOrderGateways.domain.model.tradebot.TokenLabels;

/**
 * One tradable leg, identified the way its own venue identifies it.
 *
 * <p>Construct these to say what you want to watch or trade; everything else in the library routes
 * off them. The shapes differ because the venues do: Polymarket has one book per outcome, so the
 * asset id alone is the leg. Kalshi and PredictFun have one book per market covering both outcomes,
 * so the side has to be carried separately — without it "buy the NO leg" could not be expressed.
 *
 * <p>Sealed: the three implementations below are the whole set.
 */
public sealed interface Token {

    /**
     * Which venue owns this leg.
     */
    Exchange exchange();

    /**
     * Short human-readable form for logs and error messages. Not an identifier — do not parse it.
     */
    String label();

    /**
     * Which side of a two-outcome market a leg is on.
     *
     * <p>The lower-case spelling is stored rather than derived: label() sits on the per-tick path,
     * and {@code name().toLowerCase()} would allocate a fresh string on every call for a value with
     * two possible answers.
     */
    enum Outcome {
        YES("yes"),
        NO("no");

        private final String lowerCase;

        Outcome(String lowerCase) {
            this.lowerCase = lowerCase;
        }

        public String lowerCase() {
            return lowerCase;
        }
    }

    /**
     * A Polymarket leg.
     *
     * @param assetId the ERC-1155 outcome token id, as a decimal string
     */
    record Polymarket(String assetId) implements Token {
        @Override
        public Exchange exchange() {
            return Exchange.POLYMARKET;
        }

        @Override
        public String label() {
            return TokenLabels.shorten(assetId);
        }
    }

    /**
     * A Kalshi leg. One book serves both outcomes, quoted in YES terms; a NO leg reads it mirrored,
     * which the feed does for you.
     *
     * @param marketTicker Kalshi market ticker
     * @param outcome      which side of that market this leg is
     */
    record Kalshi(String marketTicker, Outcome outcome) implements Token {
        @Override
        public Exchange exchange() {
            return Exchange.KALSHI;
        }

        @Override
        public String label() {
            return marketTicker + ":" + outcome.lowerCase();
        }
    }

    /**
     * A PredictFun leg. Quoted in YES terms like Kalshi's; the outcome token id and exchange
     * contract an order needs are resolved from venue metadata rather than carried here, because
     * they are authoritative only at the venue.
     *
     * @param marketId numeric market id, which keys both the book and the metadata lookup
     * @param outcome  which side of that market this leg is
     */
    record PredictFun(long marketId, Outcome outcome) implements Token {
        @Override
        public Exchange exchange() {
            return Exchange.PREDICTFUN;
        }

        @Override
        public String label() {
            return "pf:" + marketId + ":" + outcome.lowerCase();
        }
    }
}

