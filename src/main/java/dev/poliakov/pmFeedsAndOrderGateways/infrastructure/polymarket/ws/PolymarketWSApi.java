package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.ws;

import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

public class PolymarketWSApi {

    private static final Logger log = LoggerFactory.getLogger(PolymarketWSApi.class);

    private static final String WS_URL =
            "wss://ws-subscriptions-clob.polymarket.com/ws/market";

    private static final Duration PING_INTERVAL = Duration.ofSeconds(15);

    private final HttpClient httpClient;

    public PolymarketWSApi(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public Flux<byte[]> connect(String label, String subscribeMessage) {
        return Flux.create(sink -> {
            log.info("[{}] Connecting WS {} — subscribe message: {}", label, WS_URL, subscribeMessage);
            AtomicBoolean terminated = new AtomicBoolean(false);
            Disposable disposable = httpClient
                    .websocket()
                    .uri(WS_URL)
                    .handle((in, out) -> {
                        log.info("[{}] WS connected — sending subscribe message", label);
                        out.sendString(Mono.just(subscribeMessage)).then().subscribe();

                        Disposable pingTicker = Flux.interval(PING_INTERVAL)
                                .concatMap(tick -> out.sendObject(new PingWebSocketFrame()).then())
                                .subscribe(
                                        null,
                                        err -> log.debug("[{}] Ping send failed (connection likely closing)", label, err));

                        return in.receive()
                                .asByteArray()
                                .doOnNext(sink::next)
                                .doOnError(sink::error)
                                .doFinally(ignored -> pingTicker.dispose())
                                .then();
                    })
                    .subscribe(
                            null,
                            err -> {
                                terminated.set(true);
                                log.error("[{}] WS connection failed — server/network error", label, err);
                                sink.error(err);
                            },
                            () -> {
                                terminated.set(true);
                                log.info("[{}] WS connection closed by remote (close frame or stream ended)"
                                        + " — signalling error so the caller's retry kicks in", label);
                                sink.error(new IOException(
                                        "WS connection for [" + label + "] closed by remote"));
                            });
            sink.onDispose(() -> {
                if (terminated.compareAndSet(false, true)) {
                    log.info("[{}] WS connection cancelled locally (downstream unsubscribed, refCount grace expired)", label);
                }
                disposable.dispose();
            });
        });
    }
}

