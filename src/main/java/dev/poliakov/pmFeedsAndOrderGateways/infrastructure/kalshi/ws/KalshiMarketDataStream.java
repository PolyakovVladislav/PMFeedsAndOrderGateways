package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws.model.KalshiMarketEvent;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws.model.KalshiOrderBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class KalshiMarketDataStream implements KalshiDataStream, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KalshiMarketDataStream.class);
    private static final Duration GRACE = Duration.ofSeconds(15);

    private static final long SNAPSHOT_REQUEST_MIN_INTERVAL_MS = 5000;
    private static final int SEQUENCE_GAP_ALERT_THRESHOLD = 20;

    private static final byte[] HEARTBEAT_FRAME = "heartbeat".getBytes(StandardCharsets.UTF_8);

    private static final Sinks.EmitFailureHandler DROP =
            (signalType, result) -> result == Sinks.EmitResult.FAIL_NON_SERIALIZED;

    private final KalshiWSApi wsApi;
    private final ObjectMapper objectMapper;
    private final AtomicLong commandId = new AtomicLong();

    // One connection multiplexes every market, so the ticker isn't known until after parsing — this
    // keeps parsing off the shared Netty event loop. The scheduler is the client's single parsing
    // thread, shared with every other venue, rather than one owned here.
    private final Scheduler demuxScheduler;

    private final Map<String, KalshiOrderBook> books = new ConcurrentHashMap<>();
    private final Map<String, Sinks.Many<KalshiMarketEvent>> tickers = new ConcurrentHashMap<>();
    private final Map<String, Flux<KalshiMarketEvent>> streams = new ConcurrentHashMap<>();
    private final Map<Long, String> pendingSubscriptions = new ConcurrentHashMap<>();
    private final Map<String, Boolean> readyLogged = new ConcurrentHashMap<>();

    private final Set<String> seeded = ConcurrentHashMap.newKeySet();

    private final AtomicBoolean connectStarted = new AtomicBoolean(false);

    private final Set<String> wanted = ConcurrentHashMap.newKeySet();
    private final Object subscriptionLock = new Object();
    private boolean live;

    private Long sid;

    private Long lastSeq;
    private int sequenceGaps;
    private long lastSnapshotRequestAtMs;

    public KalshiMarketDataStream(KalshiWSApi wsApi, ObjectMapper objectMapper,
                                  Scheduler demuxScheduler) {
        this.wsApi = wsApi;
        this.objectMapper = objectMapper;
        this.demuxScheduler = demuxScheduler;
    }

    private static void applyLevels(JsonNode levelsArray, LevelApplier applier) {
        if (!levelsArray.isArray()) {
            return;
        }
        for (JsonNode level : levelsArray) {
            if (level.size() < 2) {
                continue;
            }
            applier.apply(level.get(0).asDouble(), level.get(1).asDouble());
        }
    }

    @Override
    public Flux<KalshiMarketEvent> marketStream(String marketTicker) {
        return streams.computeIfAbsent(marketTicker, this::createStream);
    }

    private void ensureConnected() {
        if (!connectStarted.compareAndSet(false, true)) {
            return;
        }
        Flux.defer(() -> wsApi.connect(new KalshiWSApi.ConnectionListener() {
                    @Override
                    public void onConnected() {
                        resubscribeAll();
                    }

                    @Override
                    public void onDisconnected() {
                        markDown();
                    }
                }))
                .publishOn(demuxScheduler)
                .subscribe(
                        this::onFrame,
                        err -> log.error("Kalshi demux pipeline terminally failed", err),
                        () -> log.warn("Kalshi demux pipeline completed unexpectedly"));
    }

    private Flux<KalshiMarketEvent> createStream(String marketTicker) {
        Sinks.Many<KalshiMarketEvent> sink = Sinks.many().multicast().directBestEffort();
        tickers.put(marketTicker, sink);
        books.put(marketTicker, new KalshiOrderBook(marketTicker));

        return sink.asFlux()
                .onBackpressureLatest()
                .doOnDiscard(KalshiMarketEvent.class,
                        e -> log.debug("[{}] Stale Kalshi tick dropped", marketTicker))
                .doOnSubscribe(s -> {
                    ensureConnected();
                    want(marketTicker);
                })
                .doFinally(signal -> {
                    log.info("[{}] Kalshi market stream torn down ({})", marketTicker, signal);
                    unwant(marketTicker);
                    books.remove(marketTicker);
                    tickers.remove(marketTicker);
                    streams.remove(marketTicker);
                    readyLogged.remove(marketTicker);
                    seeded.remove(marketTicker);
                })
                .publish()
                .refCount(1, GRACE);
    }

    private void want(String marketTicker) {
        String command;
        synchronized (subscriptionLock) {
            wanted.add(marketTicker);
            if (!live) {
                return;
            }
            command = subscribeCommand(marketTicker);
        }
        wsApi.send(command);
    }

    private void unwant(String marketTicker) {
        String command;
        synchronized (subscriptionLock) {
            wanted.remove(marketTicker);
            if (!live || sid == null) {
                return;
            }
            if (wanted.isEmpty()) {
                command = "{\"id\":" + commandId.incrementAndGet()
                        + ",\"cmd\":\"unsubscribe\",\"params\":{\"sids\":[" + sid + "]}}";
                sid = null;
            } else {
                command = "{\"id\":" + commandId.incrementAndGet()
                        + ",\"cmd\":\"update_subscription\",\"params\":{"
                        + "\"sids\":[" + sid + "],"
                        + "\"market_tickers\":[\"" + marketTicker + "\"],"
                        + "\"action\":\"delete_markets\"}}";
            }
        }
        wsApi.send(command);
    }

    private void resubscribeAll() {
        List<String> commands = new ArrayList<>();
        synchronized (subscriptionLock) {
            live = true;
            sid = null;
            pendingSubscriptions.clear();
            lastSeq = null;
            sequenceGaps = 0;
            lastSnapshotRequestAtMs = 0;
            seeded.clear();
            books.values().forEach(KalshiOrderBook::clear);
            readyLogged.clear();
            for (String marketTicker : wanted) {
                commands.add(subscribeCommand(marketTicker));
            }
        }
        commands.forEach(wsApi::send);
    }

    private void markDown() {
        synchronized (subscriptionLock) {
            live = false;
            sid = null;
            pendingSubscriptions.clear();
            lastSeq = null;
            seeded.clear();
        }
    }

    private String subscribeCommand(String marketTicker) {
        long id = commandId.incrementAndGet();
        pendingSubscriptions.put(id, marketTicker);
        log.info("[{}] Subscribing to Kalshi orderbook_delta (cmd id={})", marketTicker, id);
        return "{\"id\":" + id + ",\"cmd\":\"subscribe\",\"params\":{"
                + "\"channels\":[\"orderbook_delta\"],"
                + "\"market_ticker\":\"" + marketTicker + "\","
                + "\"use_yes_price\":true}}";
    }

    private void onSequenceGap(long previousSeq, long seq) {
        sequenceGaps++;
        long now = System.currentTimeMillis();
        boolean throttled = lastSnapshotRequestAtMs != 0
                && now - lastSnapshotRequestAtMs < SNAPSHOT_REQUEST_MIN_INTERVAL_MS;

        log.warn("Kalshi orderbook sequence gap: expected {} but got {} (gap #{}, {} missed) — {}",
                previousSeq + 1, seq, sequenceGaps, seq - previousSeq - 1,
                throttled ? "snapshot already requested recently, skipping" : "requesting fresh snapshots");

        if (sequenceGaps == SEQUENCE_GAP_ALERT_THRESHOLD) {
            log.error("{} Kalshi sequence gaps on this connection — that is far more than packet loss "
                            + "should cause; the feed may be dropping frames faster than snapshots can repair it",
                    sequenceGaps);
        }
        if (!throttled) {
            lastSnapshotRequestAtMs = now;
            requestSnapshots();
        }
    }

    private void requestSnapshots() {
        String command;
        synchronized (subscriptionLock) {
            if (sid == null || wanted.isEmpty()) {
                log.error("Cannot request Kalshi snapshots — no active sid");
                return;
            }
            String tickerList = wanted.stream()
                    .map(t -> "\"" + t + "\"")
                    .collect(Collectors.joining(","));
            command = "{\"id\":" + commandId.incrementAndGet()
                    + ",\"cmd\":\"update_subscription\",\"params\":{"
                    + "\"sids\":[" + sid + "],"
                    + "\"market_tickers\":[" + tickerList + "],"
                    + "\"action\":\"get_snapshot\"}}";
        }
        wsApi.send(command);
    }

    private void onFrame(byte[] bytes) {
        if (Arrays.equals(bytes, HEARTBEAT_FRAME)) {
            log.debug("Kalshi heartbeat received");
            return;
        }
        JsonNode root;
        try {
            root = objectMapper.readTree(bytes);
        } catch (IOException e) {
            log.error("Failed to parse Kalshi frame, raw={}", new String(bytes, StandardCharsets.UTF_8), e);
            return;
        }
        String type = root.path("type").asText(null);
        if (type == null) {
            return;
        }

        JsonNode seqNode = root.get("seq");
        if (seqNode != null && seqNode.isNumber()) {
            checkSequence(seqNode.asLong());
        }

        switch (type) {
            case "subscribed" -> handleSubscribed(root);
            case "ok" -> handleOk(root);
            case "orderbook_snapshot" -> handleSnapshot(root);
            case "orderbook_delta" -> handleDelta(root);
            case "unsubscribed" -> log.info("Kalshi unsubscribe confirmed: sid={}",
                    root.path("sid").asLong());
            case "error" -> handleError(root);
            default -> log.debug("Unhandled Kalshi WS message type={}", type);
        }
    }

    private void checkSequence(long seq) {
        Long previous = lastSeq;
        if (previous != null && seq != previous + 1) {
            onSequenceGap(previous, seq);
        }
        lastSeq = seq;
    }

    private void handleSubscribed(JsonNode root) {
        long id = root.path("id").asLong();
        long confirmedSid = root.path("msg").path("sid").asLong();
        String marketTicker = pendingSubscriptions.remove(id);
        synchronized (subscriptionLock) {
            sid = confirmedSid;
        }
        log.info("[{}] Kalshi subscription confirmed: sid={}",
                marketTicker == null ? "?" : marketTicker, confirmedSid);
    }

    private void handleOk(JsonNode root) {
        long id = root.path("id").asLong();
        String marketTicker = pendingSubscriptions.remove(id);
        JsonNode sidNode = root.get("sid");
        if (sidNode != null && sidNode.isNumber()) {
            synchronized (subscriptionLock) {
                sid = sidNode.asLong();
            }
        }
        if (marketTicker != null) {
            log.info("[{}] Kalshi subscription confirmed (added to sid={}): now covering {}",
                    marketTicker, root.path("sid").asLong(), root.path("msg").path("market_tickers"));
        } else {
            log.debug("Kalshi command ok: id={} sid={}", id, root.path("sid").asLong());
        }
    }

    private void handleError(JsonNode root) {
        long id = root.path("id").asLong();
        String marketTicker = pendingSubscriptions.remove(id);
        log.error("Kalshi WS error{}: code={} msg={}",
                marketTicker == null ? "" : " for [" + marketTicker + "]",
                root.path("msg").path("code").asInt(), root.path("msg").path("msg").asText());
    }

    private void handleSnapshot(JsonNode root) {
        JsonNode msg = root.path("msg");
        String marketTicker = msg.path("market_ticker").asText(null);
        if (marketTicker == null) {
            return;
        }
        KalshiOrderBook book = books.computeIfAbsent(marketTicker, KalshiOrderBook::new);
        book.clear();
        applyLevels(msg.path("yes_dollars_fp"), book::putYesBid);
        applyLevels(msg.path("no_dollars_fp"), book::putYesAsk);
        seeded.add(marketTicker);
        logReadyIfNeeded(marketTicker, book);
        emit(marketTicker, book, new KalshiMarketEvent.Snapshot(msg.path("ts_ms").asLong()));
    }

    private void handleDelta(JsonNode root) {
        JsonNode msg = root.path("msg");
        String marketTicker = msg.path("market_ticker").asText(null);
        String side = msg.path("side").asText(null);
        if (marketTicker == null || side == null) {
            return;
        }
        if (!seeded.contains(marketTicker)) {

            log.debug("[{}] Ignoring Kalshi delta while waiting for a snapshot", marketTicker);
            return;
        }

        double price = msg.path("price_dollars").asDouble();
        double delta = msg.path("delta_fp").asDouble();
        KalshiOrderBook book = books.computeIfAbsent(marketTicker, KalshiOrderBook::new);
        if ("yes".equals(side)) {
            book.addYesBid(price, delta);
        } else {
            book.addYesAsk(price, delta);
        }
        logReadyIfNeeded(marketTicker, book);
        emit(marketTicker, book, new KalshiMarketEvent.Delta(side, price, delta, msg.path("ts_ms").asLong()));
    }

    private void logReadyIfNeeded(String marketTicker, KalshiOrderBook book) {
        if (book.hasBothSides() && readyLogged.putIfAbsent(marketTicker, Boolean.TRUE) == null) {
            log.info("[{}] Kalshi order book ready — both sides present", marketTicker);
        }
    }

    private void emit(String marketTicker, KalshiOrderBook book, KalshiMarketEvent.Event event) {
        Sinks.Many<KalshiMarketEvent> sink = tickers.get(marketTicker);
        if (sink == null) {
            return;
        }
        sink.emitNext(
                new KalshiMarketEvent(marketTicker, book.snapshot(), event, System.currentTimeMillis()),
                DROP);
    }

    @Override
    public void close() {
        // The demux scheduler is the client's shared parsing thread and is disposed by the client.
        // Disposing it here would take every other venue's market data down with this one stream.
    }

    @FunctionalInterface
    private interface LevelApplier {
        void apply(double price, double size);
    }
}

