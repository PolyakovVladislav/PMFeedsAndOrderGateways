package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.feed;

import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Token;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.tradebot.PriceLevel;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.tradebot.TokenBookEvent;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.tradebot.TokenOrderBook;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws.KalshiDataStream;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws.model.KalshiBookSnapshot;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws.model.KalshiMarketEvent;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws.model.KalshiOrderBook;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.ws.PolymarketDataStream;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Kalshi quotes one book per market in yes-leg terms, so a NO leg reads it mirrored. Getting the
// mirror or the level ordering wrong fails silently — it just prices every order off the wrong end
// of the book.
//
// The ordering guarantee moved when the emitted value stopped being a copy of the live book: it used
// to come from yesBidLevels()/yesAskLevels() walking a TreeMap, and now comes from
// KalshiOrderBook.snapshot(). These pin it in its new home, and pin that the mirror survives it.
class KalshiBookMappingTest {

    private static final String TICKER = "TEST-TICKER";

    // Levels are inserted out of order deliberately, so the ordering is actually exercised rather
    // than accidentally satisfied by insertion order.
    private static KalshiBookSnapshot snapshot() {
        KalshiOrderBook book = new KalshiOrderBook(TICKER);
        book.putYesBid(0.40, 10);
        book.putYesBid(0.45, 5);
        book.putYesAsk(0.55, 8);
        book.putYesAsk(0.50, 12);
        return book.snapshot();
    }

    private static TokenOrderBook mapBook(KalshiBookSnapshot book, Token.Outcome outcome) {
        Token token = token(outcome);
        List<List<TokenBookEvent>> emissions = feed(book)
                .combinedStream(List.of(token))
                .collectList().block();

        assertEquals(1, emissions.size(), "expected exactly one combined emission");
        return emissions.get(0).get(0).orderBooks().get(token);
    }

    private static TokenOrderBook map(Token.Outcome outcome) {
        Token token = token(outcome);
        List<List<TokenBookEvent>> emissions = feed(snapshot())
                .combinedStream(List.of(token))
                .collectList().block();

        assertEquals(1, emissions.size(), "expected exactly one combined emission");
        List<TokenBookEvent> legs = emissions.get(0);
        assertEquals(1, legs.size(), "one leg in, one leg out");
        return legs.get(0).orderBooks().get(token);
    }

    private static Token.Kalshi token(Token.Outcome outcome) {
        return new Token.Kalshi(TICKER, outcome);
    }

    private static CombinedOrderBookFeed feed(KalshiBookSnapshot book) {
        KalshiDataStream kalshi = ticker -> Flux.just(new KalshiMarketEvent(
                ticker, book, new KalshiMarketEvent.Snapshot(1_700_000_000_000L),
                System.currentTimeMillis()));
        PolymarketDataStream polymarket = (assetIds, customFeature) -> Flux.empty();
        // No PredictFun stream: these tests only ever submit Kalshi legs, and an unconfigured venue
        // is exactly what a Polymarket+Kalshi consumer of this library has.
        return new CombinedOrderBookFeed(polymarket, kalshi, null);
    }

    @Test
    void theSnapshotIsBestPricedFirstOnBothSides() {
        KalshiBookSnapshot book = snapshot();

        assertEquals(List.of(new KalshiBookSnapshot.Level(0.45, 5),
                        new KalshiBookSnapshot.Level(0.40, 10)), book.yesBids(),
                "bids must be highest first");
        assertEquals(List.of(new KalshiBookSnapshot.Level(0.50, 12),
                        new KalshiBookSnapshot.Level(0.55, 8)), book.yesAsks(),
                "asks must be lowest first");
        assertEquals(0.45, book.bestYesBid(), 1e-9);
        assertEquals(0.50, book.bestYesAsk(), 1e-9);
    }

    @Test
    void aYesLegReadsTheBookDirectly() {
        TokenOrderBook mapped = map(Token.Outcome.YES);

        assertEquals(List.of(new PriceLevel(0.45, 5), new PriceLevel(0.40, 10)), mapped.bids());
        assertEquals(List.of(new PriceLevel(0.50, 12), new PriceLevel(0.55, 8)), mapped.asks());
    }

    @Test
    void aNoLegIsTheMirrorOfTheYesBook() {
        // Offering YES is bidding for NO: the yes ask side becomes the no bid side at 1 - p.
        TokenOrderBook mapped = map(Token.Outcome.NO);

        assertEquals(List.of(new PriceLevel(0.50, 12), new PriceLevel(0.45, 8)), mapped.bids(),
                "no bids come from yes asks mirrored");
        assertEquals(List.of(new PriceLevel(0.55, 5), new PriceLevel(0.60, 10)), mapped.asks(),
                "no asks come from yes bids mirrored");
    }

    @Test
    void theMirrorPreservesBestPricedFirstOrdering() {
        // Everything downstream walks outward from index 0. A mirror flips the sign of every
        // comparison, so this is exactly where that invariant could silently invert.
        TokenOrderBook no = map(Token.Outcome.NO);

        assertTrue(no.bids().get(0).price() > no.bids().get(1).price(), "no bids must descend");
        assertTrue(no.asks().get(0).price() < no.asks().get(1).price(), "no asks must ascend");
    }

    @Test
    void aOneSidedBookReachesTheTradingLogicRatherThanBeingHeldBack() {
        // A far-out-of-the-money leg normally quotes one side only — someone offering at a fraction
        // of a cent with nobody bidding. That is exactly the leg a sub-$1 basket wants to buy, so
        // the feed must pass it through. Whether the missing side matters is a per-direction
        // question, answered in tryTrade, not here.
        KalshiOrderBook asksOnly = new KalshiOrderBook(TICKER);
        asksOnly.putYesAsk(0.001, 500);

        TokenOrderBook mapped = mapBook(asksOnly.snapshot(), Token.Outcome.YES);

        assertEquals(List.of(new PriceLevel(0.001, 500)), mapped.asks(),
                "the tradeable side must survive");
        assertTrue(mapped.bids().isEmpty(), "the missing side stays missing rather than being faked");
    }

    @Test
    void aOneSidedYesBookIsBuyableAsYesAndSellableAsNo() {
        // One Kalshi book serves both outcomes, so a yes-side book with asks and no bids becomes a
        // NO leg with bids and no asks. Blocking one-sided books denied both of these at once.
        KalshiOrderBook asksOnly = new KalshiOrderBook(TICKER);
        asksOnly.putYesAsk(0.001, 500);

        TokenOrderBook yes = mapBook(asksOnly.snapshot(), Token.Outcome.YES);
        TokenOrderBook no = mapBook(asksOnly.snapshot(), Token.Outcome.NO);

        assertTrue(!yes.asks().isEmpty() && yes.bids().isEmpty(), "YES can be bought, not sold");
        assertTrue(!no.bids().isEmpty() && no.asks().isEmpty(), "NO can be sold, not bought");
        assertEquals(0.999, no.bids().get(0).price(), 1e-9, "the NO bid is the mirrored yes ask");
    }

    @Test
    void aBookWithNoLevelsAtAllIsStillHeldBack() {
        // The gate did not disappear, it narrowed: a book carrying no price on either side says
        // nothing and must not start a combo.
        KalshiOrderBook empty = new KalshiOrderBook(TICKER);

        List<List<TokenBookEvent>> emitted = feed(empty.snapshot())
                .combinedStream(List.of(token(Token.Outcome.YES)))
                .collectList().block();

        assertTrue(emitted == null || emitted.isEmpty(),
                "a priceless book must not reach the trading logic, got: " + emitted);
    }

    @Test
    void aClearedLevelLeavesTheSnapshotRatherThanLingeringAtZeroSize() {
        // Deltas remove a level by driving its size to zero. A snapshot that still carried it would
        // advertise depth that is not there, at the top of book of all places.
        KalshiOrderBook book = new KalshiOrderBook(TICKER);
        book.putYesBid(0.45, 5);
        book.putYesBid(0.40, 10);
        book.addYesBid(0.45, -5);

        assertEquals(List.of(new KalshiBookSnapshot.Level(0.40, 10)), book.snapshot().yesBids());
    }
}
