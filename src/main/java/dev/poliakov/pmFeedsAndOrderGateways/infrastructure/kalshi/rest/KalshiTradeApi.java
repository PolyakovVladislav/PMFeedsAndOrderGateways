package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.KalshiCredentials;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest.model.KalshiBatchOrdersRequest;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest.model.KalshiBatchOrdersResponse;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest.model.KalshiOrderResponse;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest.model.KalshiSendOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufMono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import reactor.core.scheduler.Schedulers;

public class KalshiTradeApi {

    private static final Logger log = LoggerFactory.getLogger(KalshiTradeApi.class);

    private static final String DEFAULT_BASE_URL = "https://external-api.kalshi.com/trade-api/v2";
    private static final String ORDERS_PATH = "/portfolio/events/orders";
    private static final String BATCH_ORDERS_PATH = "/portfolio/events/orders/batched";
    private static final String EXCHANGE_STATUS_PATH = "/exchange/status";

    private final HttpClient http;
    private final ObjectMapper mapper;
    private final KalshiCredentials credentials;
    private final String baseUrl;

    private final String signingPrefix;

    public KalshiTradeApi(HttpClient http,
                          ObjectMapper mapper,
                          KalshiCredentials credentials,
                          String baseUrl) {
        this.http = http;
        this.mapper = mapper;
        this.credentials = credentials;

        String resolved = (baseUrl == null || baseUrl.isBlank()) ? DEFAULT_BASE_URL : baseUrl.trim();
        while (resolved.endsWith("/")) {
            resolved = resolved.substring(0, resolved.length() - 1);
        }
        this.baseUrl = resolved;
        this.signingPrefix = URI.create(this.baseUrl).getPath();
    }

    public Mono<KalshiOrderResponse> postOrder(KalshiSendOrder order) {
        logOutgoing(ORDERS_PATH, List.of(order));
        return post(ORDERS_PATH, order)
                .map(bytes -> parse(bytes, KalshiOrderResponse.class, "KalshiOrderResponse"));
    }

    public Mono<List<KalshiOrderResponse>> postOrders(List<KalshiSendOrder> orders) {
        logOutgoing(BATCH_ORDERS_PATH, orders);
        return post(BATCH_ORDERS_PATH, new KalshiBatchOrdersRequest(orders))
                .map(bytes -> {
                    KalshiBatchOrdersResponse parsed =
                            parse(bytes, KalshiBatchOrdersResponse.class, "KalshiBatchOrdersResponse");
                    return parsed.orders() == null ? List.of() : parsed.orders();
                });
    }

    private void logOutgoing(String path, List<KalshiSendOrder> orders) {
        for (KalshiSendOrder order : orders) {
            log.info("Kalshi -> POST {} request: ticker={} side={} price={} count={} tif={} stp={}",
                    path, order.ticker(), order.side(), order.price(), order.count(),
                    order.timeInForce(), order.selfTradePreventionType());
        }
    }

    public Mono<Void> cancelOrder(String orderId) {
        String path = ORDERS_PATH + "/" + orderId;
        String label = "DELETE " + path;
        return authHeaders("DELETE", path)
                .flatMap(auth -> http
                        .headers(h -> auth.forEach(h::set))
                        .delete()
                        .uri(baseUrl + path)
                        .responseSingle((res, body) -> readAndLog(label, res, body)))
                .then();
    }

    public Mono<Void> warmup() {
        String apiKeyId = credentials.apiKeyId();
        if (apiKeyId == null || apiKeyId.isBlank()) {
            log.info("Kalshi warmup skipped — no KALSHI_API_KEY_ID configured");
            return Mono.empty();
        }
        String label = "GET " + EXCHANGE_STATUS_PATH;
        return authHeaders("GET", EXCHANGE_STATUS_PATH)
                .flatMap(auth -> http
                        .headers(h -> auth.forEach(h::set))
                        .get()
                        .uri(baseUrl + EXCHANGE_STATUS_PATH)
                        .response()
                        .doOnNext(res -> log.info("Kalshi warmup OK: {} responded {}", label, res.status()))
                        .then())
                .doOnError(err -> log.warn(
                        "Kalshi warmup failed — first order will open a fresh connection", err))
                .onErrorComplete();
    }

    private Mono<byte[]> post(String path, Object requestBody) {
        final byte[] body;
        try {
            body = mapper.writeValueAsBytes(requestBody);
        } catch (Exception e) {
            return Mono.error(new RuntimeException("Failed to serialize Kalshi request body", e));
        }
        String label = "POST " + path;
        return authHeaders("POST", path)
                .flatMap(auth -> http
                        .headers(h -> {
                            h.set("Content-Type", "application/json");
                            auth.forEach(h::set);
                        })
                        .post()
                        .uri(baseUrl + path)
                        .send((req, out) -> out.sendByteArray(Mono.just(body)))
                        .responseSingle((res, responseBody) -> readAndLog(label, res, responseBody)));
    }

    private Mono<Map<String, String>> authHeaders(String method, String path) {
        return Mono.fromSupplier(() -> {
            String timestamp = Long.toString(System.currentTimeMillis());
            String signature = credentials.sign(timestamp + method + signingPrefix + path);
            return Map.of(
                    "KALSHI-ACCESS-KEY", credentials.apiKeyId(),
                    "KALSHI-ACCESS-SIGNATURE", signature,
                    "KALSHI-ACCESS-TIMESTAMP", timestamp);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<byte[]> readAndLog(String label, HttpClientResponse res, ByteBufMono responseBody) {
        return responseBody.asString().defaultIfEmpty("").flatMap(text -> {
            log.info("Kalshi {} response [{}]: {}", label, res.status().code(), text);
            if (res.status().code() >= 400) {
                return Mono.error(new RuntimeException(
                        "Kalshi " + label + " rejected [" + res.status().code() + "]: " + text));
            }
            return Mono.just(text.getBytes(StandardCharsets.UTF_8));
        });
    }

    private <T> T parse(byte[] bytes, Class<T> type, String what) {
        try {
            return mapper.readValue(bytes, type);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse " + what, e);
        }
    }
}

