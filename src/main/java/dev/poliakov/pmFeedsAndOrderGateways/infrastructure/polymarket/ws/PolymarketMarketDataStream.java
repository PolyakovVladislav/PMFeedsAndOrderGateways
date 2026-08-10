package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.ws;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.tradebot.TokenLabels;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.ws.model.MarketEvent;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.ws.model.OrderBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PolymarketMarketDataStream implements PolymarketDataStream, AutoCloseable {

    private static final Logger log =
            LoggerFactory.getLogger(PolymarketMarketDataStream.class);
    private static final JsonFactory JSON = new JsonFactory();
    private static final Duration GRACE = Duration.ofSeconds(15);
    private static final Duration RETRY_MIN_BACKOFF = Duration.ofSeconds(1);
    private static final Duration RETRY_MAX_BACKOFF = Duration.ofSeconds(30);

    private static final Sinks.EmitFailureHandler DROP =
            (signalType, result) -> result == Sinks.EmitResult.FAIL_NON_SERIALIZED;

    private final PolymarketWSApi wsApi;

    private final Map<String, Flux<TokenTick>> tokenStreams = new ConcurrentHashMap<>();
    private final Map<String, OrderBook> books = new ConcurrentHashMap<>();
    private final Map<String, Sinks.Many<TokenTick>> tickers = new ConcurrentHashMap<>();
    private final Map<String, Boolean> readyLogged = new ConcurrentHashMap<>();
    private final Map<String, Long> lastNotReadyLogAtMs = new ConcurrentHashMap<>();

    // The client's shared parsing thread, not one owned here. Polymarket opens a connection per
    // token, and this used to mirror that with a thread per token — 18 of them in a real basket,
    // for work that measures well under one core. Kalshi always demultiplexed every market onto a
    // single thread; the asymmetry was historical rather than designed.
    private final Scheduler demuxScheduler;

    public PolymarketMarketDataStream(PolymarketWSApi wsApi, Scheduler demuxScheduler) {
        this.wsApi = wsApi;
        this.demuxScheduler = demuxScheduler;
    }

    private static String subscribeMessage(String assetId, boolean customFeatureEnabled) {
        return "{\"type\":\"market\",\"assets_ids\":[\"" + assetId
                + "\"],\"custom_feature_enabled\":" + customFeatureEnabled + "}";
    }

    private static void applyLevels(OrderBook ob, boolean bid, List<double[]> levels) {
        if (levels == null) return;
        for (double[] level : levels) {
            ob.applyLevel(bid, level[0], level[1]);
        }
    }

    private static List<double[]> parseLevels(JsonParser p) throws IOException {
        List<double[]> levels = new ArrayList<>();
        while (p.nextToken() != JsonToken.END_ARRAY) {
            double price = 0, size = 0;
            while (p.nextToken() != JsonToken.END_OBJECT) {
                if (p.currentToken() != JsonToken.FIELD_NAME) continue;
                String field = p.currentName();
                p.nextToken();
                switch (field) {
                    case "price" -> price = Double.parseDouble(p.getValueAsString());
                    case "size" -> size = Double.parseDouble(p.getValueAsString());
                    default -> p.skipChildren();
                }
            }
            levels.add(new double[]{price, size});
        }
        return levels;
    }

    private static List<String> parseStringArray(JsonParser p) throws IOException {
        List<String> result = new ArrayList<>();
        while (p.nextToken() != JsonToken.END_ARRAY) {
            result.add(p.getValueAsString());
        }
        return result;
    }

    private static List<Change> parseChanges(JsonParser p) throws IOException {
        List<Change> changes = new ArrayList<>();
        while (p.nextToken() != JsonToken.END_ARRAY) {
            String assetId = null;
            boolean bid = true;
            double price = 0, size = 0;
            while (p.nextToken() != JsonToken.END_OBJECT) {
                if (p.currentToken() != JsonToken.FIELD_NAME) continue;
                String field = p.currentName();
                p.nextToken();
                switch (field) {
                    case "asset_id" -> assetId = p.getValueAsString();
                    case "side" -> bid = "BUY".equals(p.getValueAsString());
                    case "price" -> price = Double.parseDouble(p.getValueAsString());
                    case "size" -> size = Double.parseDouble(p.getValueAsString());
                    default -> p.skipChildren();
                }
            }
            if (assetId != null) {
                changes.add(new Change(assetId, bid, price, size));
            }
        }
        return changes;
    }

    public Flux<MarketEvent> marketStream(String[] assetIds, boolean customFeatureEnabled) {
        String groupLabel = Arrays.stream(assetIds)
                .map(TokenLabels::shorten)
                .collect(Collectors.joining("+"));
        List<Flux<TokenTick>> perToken = Arrays.stream(assetIds)
                .map(id -> tokenStream(id, customFeatureEnabled))
                .toList();
        return Flux.merge(perToken)
                .handle((tick, sink) -> composeGroupEvent(assetIds, groupLabel, tick, sink));
    }

    private void composeGroupEvent(String[] assetIds, String groupLabel,
                                   TokenTick tick, SynchronousSink<MarketEvent> out) {
        Map<String, OrderBook> snap = snapshot(assetIds);
        if (tick.event() instanceof MarketEvent.MarketResolved) {
            out.next(new MarketEvent(tick.assetId(), snap, tick.event(), tick.receivedAtMs()));
            return;
        }
        if (snap.size() != assetIds.length || !allBooksUsable(snap)) {
            logNotReady(groupLabel, assetIds, snap);
            return;
        }
        out.next(new MarketEvent(tick.assetId(), snap, tick.event(), tick.receivedAtMs()));
    }

    private Flux<TokenTick> tokenStream(String assetId, boolean customFeatureEnabled) {
        return tokenStreams.computeIfAbsent(assetId, id -> createTokenStream(id, customFeatureEnabled));
    }

    private Flux<TokenTick> createTokenStream(String assetId, boolean customFeatureEnabled) {
        String label = TokenLabels.shorten(assetId);
        books.put(assetId, new OrderBook(assetId));
        Sinks.Many<TokenTick> sink = Sinks.many().multicast().directBestEffort();
        tickers.put(assetId, sink);
        // Hop off the shared Netty event loop immediately, before any parsing — that loop serves
        // every connection in the process, so parsing on it would delay all of them.
        Flux<TokenTick> frameProcessor = wsApi
                .connect(label, subscribeMessage(assetId, customFeatureEnabled))
                .publishOn(demuxScheduler)
                .doOnNext(bytes -> onFrame(assetId, bytes))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, RETRY_MIN_BACKOFF)
                        .maxBackoff(RETRY_MAX_BACKOFF)
                        .doBeforeRetry(signal -> log.warn(
                                "[{}] WS reconnect attempt #{} after: {}",
                                label, signal.totalRetries() + 1, signal.failure().toString())))
                .handle((ignored, s) -> {
                });

        return sink.asFlux()
                .onBackpressureLatest()
                .doOnDiscard(TokenTick.class, t -> log.debug("[{}] Stale tick dropped", label))
                .mergeWith(frameProcessor)
                .doFinally(signal -> {
                    log.info("[{}] Token stream torn down ({})", label, signal);
                    books.remove(assetId);
                    tickers.remove(assetId);
                    readyLogged.remove(assetId);
                    lastNotReadyLogAtMs.remove(assetId);
                    tokenStreams.remove(assetId);
                })
                .publish()
                .refCount(1, GRACE);
    }

    private void onFrame(String ownerAssetId, byte[] bytes) {
        long receivedAtMs = System.currentTimeMillis();
        try (JsonParser p = JSON.createParser(bytes)) {
            JsonToken first = p.nextToken();
            if (first == JsonToken.START_ARRAY) {
                while (p.nextToken() == JsonToken.START_OBJECT) {
                    handleEvent(ownerAssetId, p, receivedAtMs);
                }
            } else if (first == JsonToken.START_OBJECT) {
                handleEvent(ownerAssetId, p, receivedAtMs);
            }
        } catch (Exception e) {
            log.error("Failed to process Polymarket frame, raw={}", new String(bytes, StandardCharsets.UTF_8), e);
        }
    }

    private void handleEvent(String ownerAssetId, JsonParser p, long receivedAtMs) throws IOException {
        String eventType = null;
        String assetId = null;
        long sourceTs = 0;
        double directBestBid = 0, directBestAsk = 0;
        List<double[]> bidLevels = null;
        List<double[]> askLevels = null;
        List<Change> changes = null;
        List<String> resolvedAssetIds = null;
        String winningAssetId = null;
        String winningOutcome = null;

        while (p.nextToken() != JsonToken.END_OBJECT) {
            if (p.currentToken() != JsonToken.FIELD_NAME) continue;
            String field = p.currentName();
            p.nextToken();
            switch (field) {
                case "event_type" -> eventType = p.getValueAsString();
                case "asset_id" -> assetId = p.getValueAsString();
                case "timestamp" -> sourceTs = Long.parseLong(p.getValueAsString());
                case "bids" -> bidLevels = parseLevels(p);
                case "asks" -> askLevels = parseLevels(p);
                case "price_changes" -> changes = parseChanges(p);
                case "best_bid" -> directBestBid = Double.parseDouble(p.getValueAsString());
                case "best_ask" -> directBestAsk = Double.parseDouble(p.getValueAsString());
                case "assets_ids" -> resolvedAssetIds = parseStringArray(p);
                case "winning_asset_id" -> winningAssetId = p.getValueAsString();
                case "winning_outcome" -> winningOutcome = p.getValueAsString();
                default -> p.skipChildren();
            }
        }

        if ("book".equals(eventType) && ownerAssetId.equals(assetId)) {
            OrderBook fresh = new OrderBook(ownerAssetId);
            applyLevels(fresh, true, bidLevels);
            applyLevels(fresh, false, askLevels);
            books.put(ownerAssetId, fresh);
            if (!fresh.hasBothSides()) {
                log.warn("[{}] Book snapshot has only one side: bids={} asks={} — "
                                + "still emitted; only trades needing the missing side are skipped until it fills",
                        TokenLabels.shorten(ownerAssetId), fresh.bestBid(), fresh.bestAsk());
            } else {
                logBookReady(ownerAssetId);
            }
            emitTick(ownerAssetId, new MarketEvent.BookSnapshot(sourceTs), receivedAtMs);
        } else if ("price_change".equals(eventType) && changes != null) {
            boolean changed = false;
            for (Change c : changes) {
                if (!ownerAssetId.equals(c.assetId())) continue;
                OrderBook ob = books.get(ownerAssetId);
                if (ob == null) continue;
                OrderBook updated = ob.copy();
                updated.applyLevel(c.bid(), c.price(), c.size());
                books.put(ownerAssetId, updated);
                changed = true;
            }
            if (changed) {
                OrderBook ob = books.get(ownerAssetId);
                if (ob != null && ob.hasBothSides()) {
                    logBookReady(ownerAssetId);
                }
                emitTick(ownerAssetId, new MarketEvent.PriceChange(sourceTs), receivedAtMs);
            }
        } else if ("best_bid_ask".equals(eventType) && ownerAssetId.equals(assetId)
                && directBestBid > 0 && directBestAsk > 0) {
            emitTick(ownerAssetId, new MarketEvent.BestBidAsk(directBestBid, directBestAsk, sourceTs), receivedAtMs);
        } else if ("market_resolved".equals(eventType)
                && resolvedAssetIds != null && resolvedAssetIds.contains(ownerAssetId)) {
            log.info("[{}] Market resolved: outcome='{}', winningAssetId={}",
                    TokenLabels.shorten(ownerAssetId), winningOutcome, winningAssetId);
            emitTick(ownerAssetId,
                    new MarketEvent.MarketResolved(winningAssetId, winningOutcome, sourceTs), receivedAtMs);
            Sinks.Many<TokenTick> sink = tickers.get(ownerAssetId);
            if (sink != null) sink.tryEmitComplete();
        }
    }

    private void emitTick(String assetId, MarketEvent.Event event, long receivedAtMs) {
        Sinks.Many<TokenTick> sink = tickers.get(assetId);
        if (sink == null) return;
        sink.emitNext(new TokenTick(assetId, event, receivedAtMs), DROP);
    }

    private void logBookReady(String assetId) {
        if (readyLogged.putIfAbsent(assetId, Boolean.TRUE) == null) {
            log.info("[{}] Book ready — both sides present", TokenLabels.shorten(assetId));
        }
    }

    private Map<String, OrderBook> snapshot(String[] assetIds) {
        Map<String, OrderBook> snap = new HashMap<>(assetIds.length);
        for (String id : assetIds) {
            OrderBook ob = books.get(id);
            if (ob != null) snap.put(id, ob);
        }
        return Collections.unmodifiableMap(snap);
    }

    // Every asset in the group must carry a price on at least one side. A book with no levels at all
    // says nothing and would only cost a wasted evaluation downstream.
    //
    // This used to require BOTH sides of every book, which blocked a whole combo whenever any leg
    // was one-sided — the normal shape of a deep out-of-the-money leg, and precisely the leg a
    // sub-$1 basket wants to buy at 0.1c. Which side a leg actually needs is decided per direction by
    // the consumer, not here.
    private boolean allBooksUsable(Map<String, OrderBook> snap) {
        for (OrderBook ob : snap.values()) {
            if (!ob.hasAnySide()) return false;
        }
        return true;
    }

    private void logNotReady(String groupLabel, String[] assetIds, Map<String, OrderBook> snap) {
        long now = System.currentTimeMillis();
        Long last = lastNotReadyLogAtMs.get(groupLabel);
        if (last != null && now - last < 5000) return;
        lastNotReadyLogAtMs.put(groupLabel, now);
        String detail = Arrays.stream(assetIds)
                .map(id -> {
                    OrderBook ob = snap.get(id);
                    if (ob == null) return TokenLabels.shorten(id) + "[no book yet]";
                    if (!ob.hasAnySide()) {
                        return TokenLabels.shorten(id) + "[empty book, no levels either side]";
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.joining(", "));
        // Only genuinely priceless books reach this now. A one-sided book is passed through and
        // judged per direction downstream, so this line means the leg has no price at all.
        log.warn("[{}] Group emission blocked — {}", groupLabel, detail);
    }

    @Override
    public void close() {
        // The demux scheduler is the client's shared parsing thread and is disposed by the client.
        // Disposing it here would take every other venue's market data down with this one stream.
    }

    private record TokenTick(String assetId, MarketEvent.Event event, long receivedAtMs) {
    }

    private record Change(String assetId, boolean bid, double price, double size) {
    }
}

