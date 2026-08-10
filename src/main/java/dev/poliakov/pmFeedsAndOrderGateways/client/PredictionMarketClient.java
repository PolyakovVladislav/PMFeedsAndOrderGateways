package dev.poliakov.pmFeedsAndOrderGateways.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Exchange;
import dev.poliakov.pmFeedsAndOrderGateways.domain.port.out.OrderBookFeed;
import dev.poliakov.pmFeedsAndOrderGateways.domain.port.out.OrderGateway;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.feed.CombinedOrderBookFeed;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.gateway.RoutingOrderGateway;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.KalshiCredentials;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.RsaPssKalshiCredentials;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.gateway.KalshiOrderGateway;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.rest.KalshiTradeApi;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws.KalshiDataStream;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws.KalshiMarketDataStream;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws.KalshiWSApi;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.gateway.PolymarketOrderGateway;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.DerivedPolymarketCredentials;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.PolymarketClobApi;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.PolymarketCredentials;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.PolymarketNegRiskRegistry;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.ws.PolymarketDataStream;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.ws.PolymarketMarketDataStream;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.ws.PolymarketWSApi;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.EoaPredictFunCredentials;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.PredictFunCredentials;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.gateway.PredictFunOrderGateway;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.PredictFunApi;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.PredictFunMarketRegistry;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.ws.PredictFunDataStream;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.ws.PredictFunMarketDataStream;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.ws.PredictFunWSApi;
import io.netty.channel.ChannelOption;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;

/**
 * The single entry point of the library: hand it the credentials for the venues you have, and it
 * wires the whole graph — signing, REST clients, WebSocket streams — and hands back the two ports
 * the rest of your code talks to.
 *
 * <pre>{@code
 * try (PredictionMarketClient client = PredictionMarketClient.builder()
 *         .polymarket("0xPRIVATE_KEY", "0xADDRESS")
 *         .kalshi("api-key-id", "-----BEGIN PRIVATE KEY-----...")
 *         .build()) {
 *
 *     OrderBookFeed feed = client.orderBookFeed();   // subscribe to combos across venues
 *     OrderGateway gateway = client.orderGateway();  // place/cancel orders, routed by venue
 * }
 * }</pre>
 *
 * <p>You supply only what is yours — signing keys and account identifiers. Everything public (each
 * venue's exchange contract addresses, chain id and REST/WS endpoints) is built in.
 *
 * <p>Configure only the venues you use. A leg submitted for a venue you did not configure fails
 * loudly rather than silently — the client never has credentials it was not given.
 *
 * <p>The instance owns a pooled HTTP connection provider; {@link #close()} releases it. The venue
 * WebSocket sockets are opened lazily on first subscribe and torn down when unsubscribed.
 */
public final class PredictionMarketClient implements AutoCloseable {

    private final OrderBookFeed orderBookFeed;
    private final OrderGateway orderGateway;
    private final ConnectionProvider restConnectionProvider;
    private final ClientSchedulers schedulers;

    private PredictionMarketClient(OrderBookFeed orderBookFeed, OrderGateway orderGateway,
                                   ConnectionProvider restConnectionProvider,
                                   ClientSchedulers schedulers) {
        this.orderBookFeed = orderBookFeed;
        this.orderGateway = orderGateway;
        this.restConnectionProvider = restConnectionProvider;
        this.schedulers = schedulers;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Combined order-book feed over every configured venue. Subscribe with a list of tokens and it
     * emits one snapshot per tick once every leg has data.
     *
     * @return the feed, shared by every caller on this client
     */
    public OrderBookFeed orderBookFeed() {
        return orderBookFeed;
    }

    /**
     * Order gateway routed by venue: each leg of a combo is sent to the exchange that owns its
     * token. Placement across two venues is concurrent, not atomic — fills must be verified.
     *
     * @return the gateway, shared by every caller on this client
     */
    public OrderGateway orderGateway() {
        return orderGateway;
    }

    /**
     * Releases the pooled REST connections and the parsing thread. WebSocket streams close
     * themselves when their subscribers go away.
     *
     * <p>The thread is a daemon, so forgetting this leaks rather than blocking JVM shutdown — but
     * an unclosed client keeps parsing whatever is still subscribed.
     */
    @Override
    public void close() {
        restConnectionProvider.disposeLater().block(Duration.ofSeconds(5));
        schedulers.close();
    }

    public static final class Builder {

        // Polymarket's exchange contracts: public, identical for every trader, and the exact values
        // the source project runs in production — its environment leaves these unset, so these are
        // the defaults that actually sign live orders there. Proven correct by the venue *accepting*
        // orders signed against them: a wrong verifying contract fails at signature check and comes
        // back rejected, never as DELAYED. Override with the four-argument polymarket(...) for a
        // different deployment.
        private static final String POLYMARKET_EXCHANGE =
                "0xE111180000d2663C0091e4f400237545B87B996B";
        private static final String POLYMARKET_NEG_RISK_EXCHANGE =
                "0xe2222d279d744050d28e00520010520000310F59";

        // Polymarket
        private boolean polymarketEnabled;
        private String pmPrivateKey;
        private String pmAddress;
        private String pmExchangeContract = POLYMARKET_EXCHANGE;
        private String pmNegRiskExchangeContract = POLYMARKET_NEG_RISK_EXCHANGE;

        // Kalshi
        private boolean kalshiEnabled;
        private String kalshiApiKeyId;
        private String kalshiPrivateKeyPem;
        private String kalshiApiUrl = "";
        private String kalshiWsUrl = "";

        // PredictFun — its contract addresses, chain id and account address are all resolved inside
        // EoaPredictFunCredentials (address is derived from the private key), so only these two are
        // the caller's to give.
        private boolean predictFunEnabled;
        private String predictFunApiKey;
        private String predictFunPrivateKey;

        private Builder() {
        }

        private static String require(String value, String field) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException(field + " is required");
            }
            return value;
        }

        /**
         * Enables Polymarket with its standard exchange contracts.
         *
         * @param privateKey EOA signing key, hex, {@code 0x}-prefixed
         * @param address    funder address the orders trade from
         */
        public Builder polymarket(String privateKey, String address) {
            this.polymarketEnabled = true;
            this.pmPrivateKey = privateKey;
            this.pmAddress = address;
            return this;
        }

        /**
         * Enables Polymarket against non-standard exchange contracts — a different chain or a test
         * deployment. The two addresses are the EIP-712 verifying contracts orders are signed
         * against; the common case wants {@link #polymarket(String, String)} instead.
         *
         * @param privateKey              EOA signing key, hex, {@code 0x}-prefixed
         * @param address                 funder address the orders trade from
         * @param exchangeContract        CTF Exchange address
         * @param negRiskExchangeContract Neg-Risk CTF Exchange address
         */
        public Builder polymarket(String privateKey, String address,
                                  String exchangeContract, String negRiskExchangeContract) {
            this.polymarketEnabled = true;
            this.pmPrivateKey = privateKey;
            this.pmAddress = address;
            this.pmExchangeContract = exchangeContract;
            this.pmNegRiskExchangeContract = negRiskExchangeContract;
            return this;
        }

        /**
         * Enables Kalshi against its production endpoints.
         *
         * @param apiKeyId      Kalshi API key id
         * @param privateKeyPem RSA private key in PKCS#8 PEM form, used for RSA-PSS request signing
         */
        public Builder kalshi(String apiKeyId, String privateKeyPem) {
            this.kalshiEnabled = true;
            this.kalshiApiKeyId = apiKeyId;
            this.kalshiPrivateKeyPem = privateKeyPem;
            return this;
        }

        /**
         * Overrides Kalshi's endpoints, for example to point at the demo environment.
         *
         * @param apiUrl REST base URL; blank keeps the production default
         * @param wsUrl  WebSocket URL; blank keeps the production default
         */
        public Builder kalshiEndpoints(String apiUrl, String wsUrl) {
            this.kalshiApiUrl = apiUrl == null ? "" : apiUrl;
            this.kalshiWsUrl = wsUrl == null ? "" : wsUrl;
            return this;
        }

        /**
         * Enables PredictFun on BNB mainnet with its standard contracts. The trading address is
         * derived from the signing key.
         *
         * @param apiKey     PredictFun API key
         * @param privateKey EOA signing key, hex, {@code 0x}-prefixed
         */
        public Builder predictFun(String apiKey, String privateKey) {
            this.predictFunEnabled = true;
            this.predictFunApiKey = apiKey;
            this.predictFunPrivateKey = privateKey;
            return this;
        }

        public PredictionMarketClient build() {
            if (!polymarketEnabled && !kalshiEnabled && !predictFunEnabled) {
                throw new IllegalStateException(
                        "Configure at least one venue: polymarket(...), kalshi(...) or predictFun(...)");
            }

            ObjectMapper mapper = JsonMapper.builder().findAndAddModules().build();

            // Two HTTP clients, tuned as in the source project. WS wants only TCP_NODELAY; REST is a
            // small pooled client that negotiates H2 and bounds every stage so a hung venue cannot
            // wedge the order path.
            HttpClient wsHttpClient = HttpClient.create()
                    .option(ChannelOption.TCP_NODELAY, true);

            ConnectionProvider restConnectionProvider = ConnectionProvider.builder("pm-rest")
                    .maxConnections(4)
                    .maxIdleTime(Duration.ofMinutes(5))
                    .maxLifeTime(Duration.ofMinutes(10))
                    .pendingAcquireTimeout(Duration.ofMillis(100))
                    .evictInBackground(Duration.ofSeconds(30))
                    .build();
            HttpClient restHttpClient = HttpClient.create(restConnectionProvider)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .protocol(HttpProtocol.H2, HttpProtocol.HTTP11)
                    .resolver(spec -> spec
                            .cacheMaxTimeToLive(Duration.ofMinutes(5))
                            .cacheMinTimeToLive(Duration.ofSeconds(30)))
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
                    .responseTimeout(Duration.ofMillis(3000));

            // One thread parses every venue's book (see ClientSchedulers). Each stream takes it and
            // hops onto it off the Netty event loop.
            ClientSchedulers schedulers = new ClientSchedulers();

            // A stream stays null for a venue that was not configured; CombinedOrderBookFeed guards
            // each leg, so an unconfigured venue only ever fails if a leg is actually submitted for it.
            PolymarketDataStream pmStream = null;
            KalshiDataStream kalshiStream = null;
            PredictFunDataStream predictFunStream = null;
            Map<Exchange, OrderGateway> gateways = new EnumMap<>(Exchange.class);

            if (polymarketEnabled) {
                PolymarketCredentials creds = new DerivedPolymarketCredentials(
                        restHttpClient, mapper,
                        require(pmPrivateKey, "polymarket privateKey"),
                        require(pmAddress, "polymarket address"),
                        require(pmExchangeContract, "polymarket exchangeContract"),
                        require(pmNegRiskExchangeContract, "polymarket negRiskExchangeContract"));
                PolymarketClobApi clob = new PolymarketClobApi(restHttpClient, mapper, creds);
                PolymarketNegRiskRegistry negRisk = new PolymarketNegRiskRegistry(clob);
                gateways.put(Exchange.POLYMARKET, new PolymarketOrderGateway(clob, creds, negRisk));
                pmStream = new PolymarketMarketDataStream(
                        new PolymarketWSApi(wsHttpClient), schedulers.main());
            }

            if (kalshiEnabled) {
                KalshiCredentials creds = new RsaPssKalshiCredentials(
                        require(kalshiApiKeyId, "kalshi apiKeyId"),
                        require(kalshiPrivateKeyPem, "kalshi privateKeyPem"));
                KalshiTradeApi tradeApi = new KalshiTradeApi(restHttpClient, mapper, creds, kalshiApiUrl);
                gateways.put(Exchange.KALSHI, new KalshiOrderGateway(tradeApi));
                kalshiStream = new KalshiMarketDataStream(
                        new KalshiWSApi(wsHttpClient, creds, kalshiWsUrl), mapper, schedulers.main());
            }

            if (predictFunEnabled) {
                // chainId 0 and blank contracts tell EoaPredictFunCredentials to use its built-in
                // BNB-mainnet defaults; blank URLs likewise resolve to predict.fun's endpoints.
                PredictFunCredentials creds = new EoaPredictFunCredentials(
                        require(predictFunApiKey, "predictFun apiKey"),
                        require(predictFunPrivateKey, "predictFun privateKey"),
                        0, "", "", "", "");
                PredictFunApi api = new PredictFunApi(restHttpClient, mapper, creds, "");
                PredictFunMarketRegistry registry = new PredictFunMarketRegistry(api);
                gateways.put(Exchange.PREDICTFUN, new PredictFunOrderGateway(api, creds, registry));
                predictFunStream = new PredictFunMarketDataStream(
                        new PredictFunWSApi(wsHttpClient, creds, ""), mapper, schedulers.main());
            }

            OrderBookFeed feed = new CombinedOrderBookFeed(pmStream, kalshiStream, predictFunStream);
            OrderGateway gateway = new RoutingOrderGateway(gateways);
            return new PredictionMarketClient(feed, gateway, restConnectionProvider, schedulers);
        }
    }
}
