package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.PredictFunCredentials;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.crypto.ECKeyPair;
import org.web3j.crypto.Sign;
import org.web3j.utils.Numeric;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufMono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class PredictFunApi {

    private static final Logger log = LoggerFactory.getLogger(PredictFunApi.class);

    private static final String DEFAULT_API_URL = "https://api.predict.fun";

    private final HttpClient http;
    private final ObjectMapper mapper;
    private final PredictFunCredentials credentials;
    private final String baseUrl;

    private final AtomicReference<String> jwt = new AtomicReference<>();

    public PredictFunApi(HttpClient http, ObjectMapper mapper, PredictFunCredentials credentials,
                         String apiUrl) {
        this.http = http;
        this.mapper = mapper;
        this.credentials = credentials;
        String resolved = (apiUrl == null || apiUrl.isBlank()) ? DEFAULT_API_URL : apiUrl.trim();
        while (resolved.endsWith("/")) {
            resolved = resolved.substring(0, resolved.length() - 1);
        }
        this.baseUrl = resolved;
    }

    private static boolean isUnauthorized(Throwable err) {
        return err instanceof PredictFunHttpException http && http.statusCode() == 401;
    }

    public Mono<PredictFunCreateOrderResponse> createOrder(PredictFunCreateOrderRequest request) {
        logOutgoing(request);
        return withJwt(token -> {
            byte[] body = serialize(request);
            return http
                    .headers(h -> {
                        h.set("Content-Type", "application/json");
                        applyApiKey(h);
                        h.set("Authorization", "Bearer " + token);
                    })
                    .post()
                    .uri(baseUrl + "/v1/orders")
                    .send((req, out) -> out.sendByteArray(Mono.just(body)))
                    .responseSingle((res, buf) -> readBody("POST /v1/orders", res, buf))
                    .map(bytes -> parse(bytes, PredictFunCreateOrderResponse.class));
        });
    }

    public Mono<List<PredictFunOrderStateResponse>> getOrders() {
        return withJwt(token -> http
                .headers(h -> {
                    applyApiKey(h);
                    h.set("Authorization", "Bearer " + token);
                })
                .get()
                .uri(baseUrl + "/v1/orders")
                .responseSingle((res, buf) -> readBody("GET /v1/orders", res, buf))
                .map(bytes -> {
                    PredictFunOrdersResponse parsed = parse(bytes, PredictFunOrdersResponse.class);
                    return parsed.data() == null ? List.of() : parsed.data();
                }));
    }

    public Mono<PredictFunMarketResponse.Market> getMarket(long marketId) {
        String path = "/v1/markets/" + marketId;
        return http
                .headers(this::applyApiKey)
                .get()
                .uri(baseUrl + path)
                .responseSingle((res, buf) -> readBody("GET " + path, res, buf))
                .map(bytes -> {
                    PredictFunMarketResponse.Market market =
                            parse(bytes, PredictFunMarketResponse.class).data();
                    if (market == null) {
                        throw new IllegalStateException(
                                "PredictFun market " + marketId + " returned no data");
                    }
                    return market;
                });
    }

    public Mono<Void> warmup() {
        return authenticate()
                .doOnNext(token -> log.info("PredictFun warmup OK — authenticated as {}",
                        credentials.address()))
                .doOnError(err -> log.warn("PredictFun warmup failed — the first order will "
                        + "authenticate inline", err))
                .then()
                .onErrorResume(err -> Mono.empty());
    }

    private <T> Mono<T> withJwt(java.util.function.Function<String, Mono<T>> action) {
        return Mono.defer(() -> {
            String cached = jwt.get();
            return cached != null ? Mono.just(cached) : authenticate();
        }).flatMap(action).onErrorResume(err -> {
            if (!isUnauthorized(err)) {
                return Mono.error(err);
            }
            log.info("PredictFun rejected the cached JWT — re-authenticating once and retrying");
            jwt.set(null);
            return authenticate().flatMap(action);
        });
    }

    private Mono<String> authenticate() {
        return fetchAuthMessage().flatMap(message -> {
            String signature = signPersonalMessage(message);
            byte[] body = serialize(new PredictFunAuthRequest(credentials.address(), message, signature));
            return http
                    .headers(h -> {
                        h.set("Content-Type", "application/json");
                        applyApiKey(h);
                    })
                    .post()
                    .uri(baseUrl + "/v1/auth")
                    .send((req, out) -> out.sendByteArray(Mono.just(body)))
                    .responseSingle((res, buf) -> readBody("POST /v1/auth", res, buf))
                    .map(bytes -> {
                        String token = parse(bytes, PredictFunAuthResponse.class).token();
                        if (token == null || token.isBlank()) {
                            throw new IllegalStateException("PredictFun /v1/auth returned no token");
                        }
                        jwt.set(token);
                        return token;
                    });
        });
    }

    private Mono<String> fetchAuthMessage() {
        return http
                .headers(this::applyApiKey)
                .get()
                .uri(baseUrl + "/v1/auth/message")
                .responseSingle((res, buf) -> readBody("GET /v1/auth/message", res, buf))
                .map(bytes -> {
                    String message = parse(bytes, PredictFunAuthResponse.class).message();
                    if (message == null || message.isBlank()) {
                        throw new IllegalStateException(
                                "PredictFun /v1/auth/message returned no message to sign");
                    }
                    return message;
                });
    }

    private String signPersonalMessage(String message) {
        byte[] payload = message.getBytes(StandardCharsets.UTF_8);
        String hex = credentials.privateKey().startsWith("0x")
                ? credentials.privateKey().substring(2)
                : credentials.privateKey();
        ECKeyPair keyPair = ECKeyPair.create(new BigInteger(hex, 16));
        Sign.SignatureData sig = Sign.signPrefixedMessage(payload, keyPair);

        byte[] signature = new byte[65];
        System.arraycopy(sig.getR(), 0, signature, 0, 32);
        System.arraycopy(sig.getS(), 0, signature, 32, 32);
        signature[64] = sig.getV()[0];
        return Numeric.toHexString(signature);
    }

    private void applyApiKey(io.netty.handler.codec.http.HttpHeaders headers) {
        String key = credentials.apiKey();
        if (key != null && !key.isBlank()) {
            headers.set("x-api-key", key);
        }
    }

    private void logOutgoing(PredictFunCreateOrderRequest request) {
        var order = request.data().order();
        log.info("PredictFun -> POST /v1/orders request: tokenId={} side={} makerAmount={} "
                        + "takerAmount={} strategy={} pricePerShare={}",
                order.tokenId(), order.side(), order.makerAmount(), order.takerAmount(),
                request.data().strategy(), request.data().pricePerShare());
    }

    private Mono<byte[]> readBody(String label, HttpClientResponse res, ByteBufMono body) {
        return body.asString().defaultIfEmpty("").flatMap(text -> {
            int code = res.status().code();
            log.info("{} response [{}]: {}", label, code,
                    label.contains("/auth") && code < 400 ? "<credentials redacted>" : text);
            if (code >= 400) {
                return Mono.error(new PredictFunHttpException(code,
                        label + " rejected [" + code + "]: " + text));
            }
            return Mono.just(text.getBytes(StandardCharsets.UTF_8));
        });
    }

    private byte[] serialize(Object value) {
        try {
            return mapper.writeValueAsBytes(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize " + value.getClass().getSimpleName(), e);
        }
    }

    private <T> T parse(byte[] bytes, Class<T> type) {
        try {
            return mapper.readValue(bytes, type);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse " + type.getSimpleName(), e);
        }
    }

    public static class PredictFunHttpException extends RuntimeException {
        private final int statusCode;

        PredictFunHttpException(int statusCode, String message) {
            super(message);
            this.statusCode = statusCode;
        }

        public int statusCode() {
            return statusCode;
        }
    }
}

