package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws;

import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.KalshiCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.WebsocketClientSpec;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class KalshiWSApi {

    private static final Logger log = LoggerFactory.getLogger(KalshiWSApi.class);
    private static final String DEFAULT_WS_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
    private static final Duration RETRY_MIN_BACKOFF = Duration.ofSeconds(1);
    private static final Duration RETRY_MAX_BACKOFF = Duration.ofSeconds(30);

    private static final Sinks.EmitFailureHandler RETRY_EMIT =
            Sinks.EmitFailureHandler.busyLooping(Duration.ofMillis(10));

    private final HttpClient httpClient;
    private final KalshiCredentials credentials;
    private final String wsUrl;
    private final String wsPath;

    private final AtomicReference<Sinks.Many<String>> currentOutbound = new AtomicReference<>();

    public KalshiWSApi(HttpClient httpClient, KalshiCredentials credentials, String wsUrl) {
        this.httpClient = httpClient;
        this.credentials = credentials;

        String resolved = (wsUrl == null || wsUrl.isBlank()) ? DEFAULT_WS_URL : wsUrl.trim();
        while (resolved.endsWith("/")) {
            resolved = resolved.substring(0, resolved.length() - 1);
        }
        this.wsUrl = resolved;
        this.wsPath = URI.create(this.wsUrl).getPath();
    }

    public Flux<byte[]> connect(ConnectionListener listener) {
        return Flux.<byte[]>create(sink -> {
                    Sinks.Many<String> outbound = Sinks.many().unicast().onBackpressureBuffer();
                    currentOutbound.set(outbound);
                    Map<String, String> headers = handshakeHeaders();
                    AtomicBoolean down = new AtomicBoolean(false);
                    Runnable markDown = () -> {
                        if (down.compareAndSet(false, true)) {
                            listener.onDisconnected();
                        }
                    };

                    Disposable disposable = httpClient
                            .headers(h -> {
                                headers.forEach(h::set);

                                h.set("Origin", "");
                            })
                            .websocket(WebsocketClientSpec.builder().handlePing(true).build())
                            .uri(wsUrl)
                            .handle((in, out) -> {
                                log.info("Kalshi WS connected — {}", wsUrl);
                                out.sendString(outbound.asFlux()).then().subscribe();
                                listener.onConnected();
                                return in.receive()
                                        .asByteArray()
                                        .doOnNext(sink::next)
                                        .doOnError(sink::error)
                                        .then();
                            })
                            .subscribe(
                                    null,
                                    err -> {
                                        markDown.run();
                                        log.error("Kalshi WS connection failed — server/network error", err);
                                        sink.error(err);
                                    },
                                    () -> {
                                        markDown.run();
                                        log.info("Kalshi WS connection closed by remote — signalling error "
                                                + "so the retry kicks in");
                                        sink.error(new IOException("Kalshi WS connection closed by remote"));
                                    });
                    sink.onDispose(() -> {
                        if (!down.get()) {
                            log.info("Kalshi WS connection cancelled locally");
                        }
                        markDown.run();
                        disposable.dispose();
                    });
                })
                .retryWhen(Retry.backoff(Long.MAX_VALUE, RETRY_MIN_BACKOFF)
                        .maxBackoff(RETRY_MAX_BACKOFF)
                        .doBeforeRetry(signal -> log.warn("Kalshi WS reconnect attempt #{} after: {}",
                                signal.totalRetries() + 1, signal.failure().toString())));
    }

    public void send(String textFrame) {
        Sinks.Many<String> outbound = currentOutbound.get();
        if (outbound == null) {
            log.warn("Kalshi WS send() called before any connection attempt — dropping frame: {}", textFrame);
            return;
        }
        outbound.emitNext(textFrame, RETRY_EMIT);
    }

    private Map<String, String> handshakeHeaders() {
        String timestamp = Long.toString(System.currentTimeMillis());
        String signature = credentials.sign(timestamp + "GET" + wsPath);
        return Map.of(
                "KALSHI-ACCESS-KEY", credentials.apiKeyId(),
                "KALSHI-ACCESS-SIGNATURE", signature,
                "KALSHI-ACCESS-TIMESTAMP", timestamp);
    }

    public interface ConnectionListener {
        void onConnected();

        void onDisconnected();
    }
}

