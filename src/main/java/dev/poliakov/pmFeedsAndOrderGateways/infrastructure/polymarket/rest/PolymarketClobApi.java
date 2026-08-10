package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.gateway.Eip712AuthSigner;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.gateway.Eip712Utils;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufMono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

public class PolymarketClobApi {

    private static final Logger log = LoggerFactory.getLogger(PolymarketClobApi.class);

    private static final String CLOB_BASE = "https://clob.polymarket.com";

    private final HttpClient http;
    private final ObjectMapper mapper;
    private final PolymarketCredentials credentials;

    private volatile String eoaAddress;

    public PolymarketClobApi(HttpClient http,
                             ObjectMapper mapper,
                             PolymarketCredentials credentials) {
        this.http = http;
        this.mapper = mapper;
        this.credentials = credentials;
    }

    // Empty when the venue answered but told us nothing about the order.
    //
    // The CLOB replies 200 with a literal `null` body for an order it has accepted and not yet
    // indexed. Jackson maps that to null, and returning null from map() is illegal in Reactor — it
    // threw "the mapper returned a null value" and killed the caller's poll loop on its very first
    // attempt, turning a few hundred milliseconds of indexing lag into permanent manual
    // reconciliation.
    //
    // Empty rather than a zero-filled state on purpose: "the venue does not know this order yet"
    // and "the venue says it matched nothing" are different answers with opposite consequences, and
    // only the caller can decide what an exhausted poll window means. See
    // PolymarketOrderGateway.resolveFilledSize.
    //
    // Split out from the request chain so this decision is reachable from a test without standing
    // up an HTTP server — it is the whole of the fix, and it earned coverage the hard way.
    static Mono<OrderStateResponse> decodeOrderState(byte[] bytes, ObjectMapper mapper) {
        OrderStateResponse state;
        try {
            state = mapper.readValue(bytes, OrderStateResponse.class);
        } catch (Exception e) {
            return Mono.error(new RuntimeException("Failed to parse OrderStateResponse", e));
        }
        return state == null ? Mono.empty() : Mono.just(state);
    }

    private String eoaAddress() {
        String cached = eoaAddress;
        if (cached == null) {
            cached = Eip712Utils.deriveAddress(credentials.privateKey());
            eoaAddress = cached;
        }
        return cached;
    }

    private void logOutgoing(String path, List<SendOrder> sendOrders) {
        for (SendOrder sendOrder : sendOrders) {
            ClobOrder order = sendOrder.order();
            log.info("Polymarket -> POST {} request: tokenId={} side={} makerAmount={} takerAmount={} "
                            + "orderType={}",
                    path, order.tokenId(), order.side(), order.makerAmount(), order.takerAmount(),
                    sendOrder.orderType());
        }
    }

    public Mono<OrderResponse> postOrder(SendOrder sendOrder) {
        logOutgoing("/order", List.of(sendOrder));
        final byte[] body;
        try {
            body = mapper.writeValueAsBytes(sendOrder);
        } catch (Exception e) {
            return Mono.error(new RuntimeException("Failed to serialize SendOrder", e));
        }
        String timestamp = String.valueOf(System.currentTimeMillis() / 1000L);
        String signature = hmacSignature(timestamp, "POST", "/order", body);
        return http
                .headers(h -> h
                        .set("Content-Type", "application/json")
                        .set("POLY_API_KEY", credentials.apiKey())
                        .set("POLY_ADDRESS", eoaAddress())
                        .set("POLY_PASSPHRASE", credentials.passphrase())
                        .set("POLY_TIMESTAMP", timestamp)
                        .set("POLY_SIGNATURE", signature))
                .post()
                .uri(CLOB_BASE + "/order")
                .send((req, out) -> out.sendByteArray(Mono.just(body)))
                .responseSingle((res, responseBody) -> readAndLog("POST /order", res, responseBody))
                .map(bytes -> {
                    try {
                        return mapper.readValue(bytes, OrderResponse.class);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse OrderResponse", e);
                    }
                });
    }

    public Mono<List<OrderResponse>> postOrders(List<SendOrder> sendOrders) {
        logOutgoing("/orders", sendOrders);
        final byte[] body;
        try {
            body = mapper.writeValueAsBytes(sendOrders);
        } catch (Exception e) {
            return Mono.error(new RuntimeException("Failed to serialize bulk SendOrder list", e));
        }
        String timestamp = String.valueOf(System.currentTimeMillis() / 1000L);
        String signature = hmacSignature(timestamp, "POST", "/orders", body);
        return http
                .headers(h -> h
                        .set("Content-Type", "application/json")
                        .set("POLY_API_KEY", credentials.apiKey())
                        .set("POLY_ADDRESS", eoaAddress())
                        .set("POLY_PASSPHRASE", credentials.passphrase())
                        .set("POLY_TIMESTAMP", timestamp)
                        .set("POLY_SIGNATURE", signature))
                .post()
                .uri(CLOB_BASE + "/orders")
                .send((req, out) -> out.sendByteArray(Mono.just(body)))
                .responseSingle((res, responseBody) -> readAndLog("POST /orders", res, responseBody))
                .map(bytes -> {
                    try {
                        return mapper.readValue(bytes,
                                mapper.getTypeFactory().constructCollectionType(List.class, OrderResponse.class));
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse bulk OrderResponse list", e);
                    }
                });
    }

    public Mono<Boolean> fetchNegRisk(String tokenId) {
        String path = "/book?token_id=" + tokenId;
        return http
                .get()
                .uri(CLOB_BASE + path)
                .responseSingle((res, body) -> body.asString().defaultIfEmpty("").flatMap(text -> {
                    if (res.status().code() >= 400) {
                        return Mono.error(new RuntimeException(
                                "GET " + path + " rejected [" + res.status().code() + "]: " + text));
                    }
                    return Mono.just(text);
                }))
                .map(text -> {
                    JsonNode negRisk;
                    try {
                        negRisk = mapper.readTree(text).get("neg_risk");
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse book for token " + tokenId, e);
                    }
                    if (negRisk == null || !negRisk.isBoolean()) {
                        throw new IllegalStateException(
                                "Book for token " + tokenId + " carries no neg_risk flag");
                    }
                    return negRisk.asBoolean();
                });
    }

    public Mono<OrderStateResponse> getOrder(String orderId) {
        String path = "/data/order/" + orderId;
        String timestamp = String.valueOf(System.currentTimeMillis() / 1000L);
        String signature = hmacSignature(timestamp, "GET", path, new byte[0]);
        return http
                .headers(h -> h
                        .set("POLY_API_KEY", credentials.apiKey())
                        .set("POLY_ADDRESS", eoaAddress())
                        .set("POLY_PASSPHRASE", credentials.passphrase())
                        .set("POLY_TIMESTAMP", timestamp)
                        .set("POLY_SIGNATURE", signature))
                .get()
                .uri(CLOB_BASE + path)
                .responseSingle((res, body) -> readAndLog("GET " + path, res, body))
                .flatMap(bytes -> decodeOrderState(bytes, mapper));
    }

    public Mono<Void> cancelOrder(String orderId) {
        String path = "/order/" + orderId;
        String timestamp = String.valueOf(System.currentTimeMillis() / 1000L);
        String signature = hmacSignature(timestamp, "DELETE", path, new byte[0]);
        return http
                .headers(h -> h
                        .set("POLY_API_KEY", credentials.apiKey())
                        .set("POLY_ADDRESS", eoaAddress())
                        .set("POLY_PASSPHRASE", credentials.passphrase())
                        .set("POLY_TIMESTAMP", timestamp)
                        .set("POLY_SIGNATURE", signature))
                .delete()
                .uri(CLOB_BASE + path)
                .responseSingle((res, body) -> readAndLog("DELETE /order/" + orderId, res, body))
                .then();
    }

    private Mono<byte[]> readAndLog(String label, HttpClientResponse res, ByteBufMono responseBody) {
        return responseBody.asString().defaultIfEmpty("").flatMap(text -> {
            log.info("{} response [{}]: {}", label, res.status().code(), text);
            if (res.status().code() >= 400) {
                return Mono.error(new RuntimeException(
                        label + " rejected [" + res.status().code() + "]: " + text));
            }
            return Mono.just(text.getBytes(StandardCharsets.UTF_8));
        });
    }

    public Mono<ApiKeyCredentials> createApiKey(long nonce) {
        return l1Request(true, "/auth/api-key", nonce);
    }

    public Mono<ApiKeyCredentials> deriveApiKey(long nonce) {
        return l1Request(false, "/auth/derive-api-key", nonce);
    }

    private Mono<ApiKeyCredentials> l1Request(boolean post, String path, long nonce) {
        long timestamp = System.currentTimeMillis() / 1000L;
        Eip712AuthSigner signer = new Eip712AuthSigner(credentials.privateKey());
        String signature = signer.sign(timestamp, nonce);

        HttpClient authenticated = http.headers(h -> h
                .set("POLY_ADDRESS", signer.address())
                .set("POLY_SIGNATURE", signature)
                .set("POLY_TIMESTAMP", String.valueOf(timestamp))
                .set("POLY_NONCE", String.valueOf(nonce)));

        String label = (post ? "POST " : "GET ") + path;
        Mono<byte[]> response = (post ? authenticated.post() : authenticated.get())
                .uri(CLOB_BASE + path)
                .responseSingle((res, responseBody) -> readL1Body(label, res, responseBody));

        return response.map(bytes -> {
            try {
                return mapper.readValue(bytes, ApiKeyCredentials.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse ApiKeyCredentials", e);
            }
        });
    }

    private Mono<byte[]> readL1Body(String label, HttpClientResponse res, ByteBufMono responseBody) {
        return responseBody.asString().defaultIfEmpty("").flatMap(text -> {
            log.info("{} response [{}]: {}", label, res.status().code(),
                    res.status().code() >= 400 ? text : "<credentials redacted>");
            if (res.status().code() >= 400) {
                return Mono.error(new RuntimeException(
                        label + " rejected [" + res.status().code() + "]: " + text));
            }
            return Mono.just(text.getBytes(StandardCharsets.UTF_8));
        });
    }

    public Mono<Void> warmup() {
        return http
                .get()
                .uri(CLOB_BASE + "/")
                .response()
                .doOnNext(res -> log.info("Warmup OK: CLOB API responded {}", res.status()))
                .doOnError(err -> log.warn("Warmup failed — first order will open a fresh connection", err))
                .onErrorComplete()
                .then();
    }

    private String hmacSignature(String timestamp, String method, String path, byte[] body) {
        try {
            String message = timestamp + method + path + new String(body, StandardCharsets.UTF_8);
            byte[] keyBytes = Base64.getUrlDecoder().decode(credentials.apiSecret());
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(keyBytes, "HmacSHA256"));
            return Base64.getUrlEncoder()
                    .encodeToString(mac.doFinal(message.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to sign request", e);
        }
    }
}

