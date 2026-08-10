package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.ws.model.PredictFunMarketEvent;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.ws.model.PredictFunOrderBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class PredictFunMarketDataStream implements PredictFunDataStream, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PredictFunMarketDataStream.class);

    private static final Duration GRACE = Duration.ofSeconds(15);
    private static final String ORDERBOOK_TOPIC_PREFIX = "predictOrderbook/";
    private static final String HEARTBEAT_TOPIC = "heartbeat";

    private static final Sinks.EmitFailureHandler DROP = (signalType, emitResult) ->
            emitResult == Sinks.EmitResult.FAIL_NON_SERIALIZED;

    private final PredictFunWSApi wsApi;
    private final ObjectMapper objectMapper;

    // The client's single parsing thread, shared with every other venue, rather than one owned here.
    private final Scheduler demuxScheduler;

    private final Map<Long, Sinks.Many<PredictFunMarketEvent>> streams = new ConcurrentHashMap<>();
    private final Map<Long, Flux<PredictFunMarketEvent>> published = new ConcurrentHashMap<>();
    private final Set<Long> wanted = ConcurrentHashMap.newKeySet();
    private final Set<Long> readyLogged = ConcurrentHashMap.newKeySet();

    private final AtomicBoolean connectStarted = new AtomicBoolean(false);
    private final AtomicLong requestId = new AtomicLong(1);
    private volatile boolean live;

    public PredictFunMarketDataStream(PredictFunWSApi wsApi, ObjectMapper objectMapper,
                                      Scheduler demuxScheduler) {
        this.wsApi = wsApi;
        this.objectMapper = objectMapper;
        this.demuxScheduler = demuxScheduler;
    }

    private static List<PredictFunOrderBook.Level> levels(JsonNode array) {
        if (array == null || !array.isArray()) {
            return List.of();
        }
        List<PredictFunOrderBook.Level> levels = new ArrayList<>(array.size());
        for (JsonNode pair : array) {
            if (!pair.isArray() || pair.size() < 2) {
                continue;
            }
            double size = pair.get(1).asDouble();
            if (size > 0) {
                levels.add(new PredictFunOrderBook.Level(pair.get(0).asDouble(), size));
            }
        }
        return levels;
    }

    @Override
    public Flux<PredictFunMarketEvent> marketStream(long marketId) {
        return published.computeIfAbsent(marketId, this::createStream);
    }

    private Flux<PredictFunMarketEvent> createStream(long marketId) {
        Sinks.Many<PredictFunMarketEvent> sink = Sinks.many().multicast().directBestEffort();
        streams.put(marketId, sink);
        return sink.asFlux()
                .doOnSubscribe(s -> {
                    ensureConnected();
                    want(marketId);
                })
                .doFinally(signal -> unwant(marketId))

                .publish()
                .refCount(1, GRACE);
    }

    private void ensureConnected() {
        if (!connectStarted.compareAndSet(false, true)) {
            return;
        }
        wsApi.connect(new PredictFunWSApi.ConnectionListener() {
                    @Override
                    public void onConnected() {
                        live = true;
                        resubscribeAll();
                    }

                    @Override
                    public void onDisconnected() {
                        live = false;
                    }
                })
                .publishOn(demuxScheduler)
                .subscribe(this::onFrame,
                        err -> log.error("PredictFun WS stream terminally failed after retries", err));
    }

    private void want(long marketId) {
        wanted.add(marketId);
        if (live) {
            subscribe(marketId);
        }
    }

    private void unwant(long marketId) {
        if (!wanted.remove(marketId)) {
            return;
        }
        readyLogged.remove(marketId);
        if (live) {
            wsApi.send(command("unsubscribe", ORDERBOOK_TOPIC_PREFIX + marketId));
        }
    }

    private void resubscribeAll() {
        List<Long> markets = new ArrayList<>(wanted);
        if (markets.isEmpty()) {
            return;
        }
        log.info("PredictFun WS re-subscribing {} market(s) after connect", markets.size());
        markets.forEach(this::subscribe);
    }

    private void subscribe(long marketId) {
        wsApi.send(command("subscribe", ORDERBOOK_TOPIC_PREFIX + marketId));
    }

    private String command(String method, String topic) {
        return "{\"method\":\"" + method + "\",\"requestId\":" + requestId.getAndIncrement()
                + ",\"params\":[\"" + topic + "\"]}";
    }

    private void onFrame(byte[] bytes) {
        try {
            JsonNode root = objectMapper.readTree(bytes);
            String type = root.path("type").asText("");
            switch (type) {
                case "M" -> onMessage(root);
                case "R" -> onResponse(root);
                default -> log.debug("PredictFun WS frame with unknown type '{}': {}", type, root);
            }
        } catch (Exception e) {
            log.error("PredictFun WS frame could not be parsed", e);
        }
    }

    private void onResponse(JsonNode root) {
        if (root.path("success").asBoolean(false)) {
            return;
        }
        JsonNode error = root.path("error");
        log.error("PredictFun WS request {} failed: code={} message={}",
                root.path("requestId").asLong(-1),
                error.path("code").asText("?"), error.path("message").asText("?"));
    }

    private void onMessage(JsonNode root) {
        String topic = root.path("topic").asText("");
        if (HEARTBEAT_TOPIC.equals(topic)) {

            wsApi.send("{\"method\":\"heartbeat\",\"data\":" + root.path("data").toString() + "}");
            return;
        }
        if (!topic.startsWith(ORDERBOOK_TOPIC_PREFIX)) {
            return;
        }
        long marketId;
        try {
            marketId = Long.parseLong(topic.substring(ORDERBOOK_TOPIC_PREFIX.length()));
        } catch (NumberFormatException e) {
            log.warn("PredictFun WS orderbook topic with unparseable market id: {}", topic);
            return;
        }
        emitBook(marketId, root.path("data"));
    }

    private void emitBook(long marketId, JsonNode data) {
        Sinks.Many<PredictFunMarketEvent> sink = streams.get(marketId);
        if (sink == null) {
            return;
        }
        PredictFunOrderBook book = new PredictFunOrderBook(
                marketId, levels(data.path("bids")), levels(data.path("asks")));

        if (readyLogged.add(marketId) && book.hasBothSides()) {
            log.info("PredictFun market {} book ready: {}", marketId, book);
        }
        sink.emitNext(new PredictFunMarketEvent(
                        marketId, book,
                        data.path("updateTimestampMs").asLong(0),
                        System.currentTimeMillis()),
                DROP);
    }

    @Override
    public void close() {
        // The demux scheduler is the client's shared parsing thread and is disposed by the client.
        // Disposing it here would take every other venue's market data down with this one stream.
        log.info("PredictFun market data stream shut down");
    }
}

