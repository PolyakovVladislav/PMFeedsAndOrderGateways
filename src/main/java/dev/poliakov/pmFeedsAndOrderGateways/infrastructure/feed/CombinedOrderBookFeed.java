package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.feed;

import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Token;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.tradebot.PriceLevel;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.tradebot.TokenBookEvent;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.tradebot.TokenOrderBook;
import dev.poliakov.pmFeedsAndOrderGateways.domain.port.out.OrderBookFeed;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws.KalshiDataStream;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws.model.KalshiBookSnapshot;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws.model.KalshiMarketEvent;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.ws.PolymarketDataStream;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.ws.model.MarketEvent;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.ws.model.OrderBook;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.ws.PredictFunDataStream;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.ws.model.PredictFunMarketEvent;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.ws.model.PredictFunOrderBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class CombinedOrderBookFeed implements OrderBookFeed {

    private static final Logger log = LoggerFactory.getLogger(CombinedOrderBookFeed.class);

    private static final boolean POLYMARKET_CUSTOM_FEATURE = false;
    // Named for the empty book it now gates, not the half book it used to: a one-sided book is no
    // longer held back, so this paces the "no levels at all" line only.
    private static final long EMPTY_BOOK_LOG_INTERVAL_MS = 30_000;

    private final Map<String, Long> lastEmptyBookLogAtMs = new ConcurrentHashMap<>();

    private final PolymarketDataStream polymarketDataStream;
    private final KalshiDataStream kalshiDataStream;
    private final PredictFunDataStream predictFunDataStream;

    public CombinedOrderBookFeed(PolymarketDataStream polymarketDataStream,
                                 KalshiDataStream kalshiDataStream,
                                 PredictFunDataStream predictFunDataStream) {
        this.polymarketDataStream = polymarketDataStream;
        this.kalshiDataStream = kalshiDataStream;
        this.predictFunDataStream = predictFunDataStream;
    }

    // A stream is null when its venue was never configured on the client. Reaching here means a leg
    // was submitted for a venue with no credentials wired — a caller mistake, so it fails loudly
    // rather than emitting a silent empty leg that would stall the whole combo.
    private static <T> T require(T stream, String venue) {
        if (stream == null) {
            throw new IllegalStateException(
                    venue + " is not configured on this client, but a " + venue + " leg was submitted");
        }
        return stream;
    }

    private static List<TokenBookEvent> toList(Object[] values) {
        return Arrays.stream(values).map(v -> (TokenBookEvent) v).toList();
    }

    private static TokenBookEvent toDomain(Token.Polymarket token, MarketEvent event) {
        OrderBook book = event.orderBooks().get(token.assetId());
        if (book == null) {
            return null;
        }
        TokenOrderBook mapped = new TokenOrderBook(
                token, toLevels(book.levels(true)), toLevels(book.levels(false)));
        return new TokenBookEvent(Map.of(token, mapped), toDomain(event.event()), event.receivedAtMs());
    }

    private static TokenBookEvent.Event toDomain(MarketEvent.Event event) {
        return switch (event) {
            case MarketEvent.BookSnapshot s -> new TokenBookEvent.BookSnapshot(s.sourceTimestamp());
            case MarketEvent.PriceChange p -> new TokenBookEvent.PriceUpdate(p.sourceTimestamp());
            case MarketEvent.BestBidAsk b -> new TokenBookEvent.PriceUpdate(b.sourceTimestamp());
            case MarketEvent.MarketResolved r -> new TokenBookEvent.MarketResolved(
                    r.winningAssetId(), r.winningOutcome(), r.sourceTimestamp());
        };
    }

    private static TokenBookEvent toDomain(Token.Kalshi token, KalshiMarketEvent event) {
        KalshiBookSnapshot book = event.orderBook();
        boolean yes = token.outcome() == Token.Outcome.YES;
        List<PriceLevel> bids = yes ? kalshiLevels(book.yesBids()) : kalshiMirrored(book.yesAsks());
        List<PriceLevel> asks = yes ? kalshiLevels(book.yesAsks()) : kalshiMirrored(book.yesBids());

        TokenBookEvent.Event mapped = switch (event.event()) {
            case KalshiMarketEvent.Snapshot s -> new TokenBookEvent.BookSnapshot(s.sourceTimestampMs());
            case KalshiMarketEvent.Delta d -> new TokenBookEvent.PriceUpdate(d.sourceTimestampMs());
        };
        return new TokenBookEvent(
                Map.of(token, new TokenOrderBook(token, bids, asks)), mapped, event.receivedAtMs());
    }

    private static TokenBookEvent toDomain(Token.PredictFun token, PredictFunMarketEvent event) {
        PredictFunOrderBook book = event.orderBook();
        boolean yes = token.outcome() == Token.Outcome.YES;
        List<PriceLevel> bids = yes ? toPfLevels(book.yesBids()) : mirroredPf(book.yesAsks());
        List<PriceLevel> asks = yes ? toPfLevels(book.yesAsks()) : mirroredPf(book.yesBids());

        return new TokenBookEvent(
                Map.of(token, new TokenOrderBook(token, bids, asks)),
                new TokenBookEvent.BookSnapshot(event.sourceTimestampMs()),
                event.receivedAtMs());
    }

    private static List<PriceLevel> toPfLevels(List<PredictFunOrderBook.Level> levels) {
        List<PriceLevel> out = new ArrayList<>(levels.size());
        for (PredictFunOrderBook.Level level : levels) {
            out.add(new PriceLevel(level.price(), level.size()));
        }
        return out;
    }

    private static List<PriceLevel> mirroredPf(List<PredictFunOrderBook.Level> levels) {
        List<PriceLevel> out = new ArrayList<>(levels.size());
        for (PredictFunOrderBook.Level level : levels) {
            out.add(new PriceLevel(round4(1.0 - level.price()), level.size()));
        }
        return out;
    }

    // Named apart from toLevels rather than overloaded: erasure makes List<Map.Entry<..>> and
    // List<Level> the same signature. Polymarket still hands over map entries.
    private static List<PriceLevel> kalshiLevels(List<KalshiBookSnapshot.Level> levels) {
        List<PriceLevel> out = new ArrayList<>(levels.size());
        for (KalshiBookSnapshot.Level level : levels) {
            out.add(new PriceLevel(level.price(), level.size()));
        }
        return out;
    }

    private static List<PriceLevel> kalshiMirrored(List<KalshiBookSnapshot.Level> levels) {
        List<PriceLevel> out = new ArrayList<>(levels.size());
        for (KalshiBookSnapshot.Level level : levels) {
            out.add(new PriceLevel(round4(1.0 - level.price()), level.size()));
        }
        return out;
    }

    // Plain loops over presized lists rather than streams, on all of these: they run once per leg
    // per book tick, which is the hottest path in the process, and a stream pipeline allocates a
    // spliterator and an intermediate buffer to do the same single pass.

    private static List<PriceLevel> toLevels(List<Map.Entry<Double, Double>> levels) {
        List<PriceLevel> out = new ArrayList<>(levels.size());
        for (Map.Entry<Double, Double> level : levels) {
            out.add(new PriceLevel(level.getKey(), level.getValue()));
        }
        return out;
    }

    private static double round4(double value) {
        return Math.round(value * 10000.0) / 10000.0;
    }

    @Override
    public Flux<List<TokenBookEvent>> combinedStream(List<Token> tokens) {
        List<Flux<TokenBookEvent>> legs = tokens.stream().map(this::legStream).toList();

        String groupLabel = tokens.stream().map(Token::label).collect(Collectors.joining(", "));
        long subscribedAtMs = System.currentTimeMillis();
        AtomicBoolean firstEmission = new AtomicBoolean(true);

        return Flux.combineLatest(legs, CombinedOrderBookFeed::toList)
                .doOnNext(events -> {
                    if (firstEmission.compareAndSet(true, false)) {
                        log.info("[{}] Combined stream ready — {}ms after subscribe",
                                groupLabel, System.currentTimeMillis() - subscribedAtMs);
                    }
                });
    }

    private Flux<TokenBookEvent> legStream(Token token) {
        return switch (token) {
            case Token.Polymarket polymarket -> require(polymarketDataStream, "Polymarket")
                    .marketStream(new String[]{polymarket.assetId()}, POLYMARKET_CUSTOM_FEATURE)
                    .handle((event, sink) -> {
                        TokenBookEvent mapped = toDomain(polymarket, event);
                        if (mapped != null) {
                            sink.next(mapped);
                        }
                    });
            case Token.Kalshi kalshi -> require(kalshiDataStream, "Kalshi")
                    .marketStream(kalshi.marketTicker())
                    .handle((event, sink) -> {
                        // Pass anything carrying a price on either side. Which side a leg needs is a
                        // per-direction question the consumer answers; requiring both here shut out
                        // one-sided far-OTM legs — the cheap legs a sub-$1 basket wants.
                        if (event.orderBook().hasAnySide()) {
                            sink.next(toDomain(kalshi, event));
                        } else {
                            logEmptyBook(kalshi, event.orderBook());
                        }
                    });
            case Token.PredictFun predictFun -> require(predictFunDataStream, "PredictFun")
                    .marketStream(predictFun.marketId())
                    .handle((event, sink) -> {
                        if (event.orderBook().hasAnySide()) {
                            sink.next(toDomain(predictFun, event));
                        } else {
                            logEmptyBook(predictFun, event.orderBook());
                        }
                    });
        };
    }

    private void logEmptyBook(Token.Kalshi token, KalshiBookSnapshot book) {
        long now = System.currentTimeMillis();
        Long last = lastEmptyBookLogAtMs.get(token.marketTicker());
        if (last != null && now - last < EMPTY_BOOK_LOG_INTERVAL_MS) {
            return;
        }
        lastEmptyBookLogAtMs.put(token.marketTicker(), now);
        log.warn("[{}] Kalshi leg held back — book has no levels at all (yesBid={} yesAsk={}); "
                        + "any combo using this leg stays idle until it quotes something",
                token.label(), book.bestYesBid(), book.bestYesAsk());
    }

    private void logEmptyBook(Token.PredictFun token, PredictFunOrderBook book) {
        long now = System.currentTimeMillis();
        String key = "pf:" + token.marketId();
        Long last = lastEmptyBookLogAtMs.get(key);
        if (last != null && now - last < EMPTY_BOOK_LOG_INTERVAL_MS) {
            return;
        }
        lastEmptyBookLogAtMs.put(key, now);
        log.warn("[{}] PredictFun leg held back — book has no levels at all (yesBid={} yesAsk={}); "
                        + "any combo using this leg stays idle until it quotes something",
                token.label(), book.bestYesBid(), book.bestYesAsk());
    }
}

