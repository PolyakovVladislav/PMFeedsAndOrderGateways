package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.model.ApiKeyCredentials;
import reactor.netty.http.client.HttpClient;

public class DerivedPolymarketCredentials implements PolymarketCredentials {

    private static final long API_KEY_NONCE = 0;

    private final String privateKey;
    private final String address;
    private final String exchangeContract;
    private final String negRiskExchangeContract;
    private final PolymarketClobApi authClobApi;

    private volatile ApiKeyCredentials derived;

    public DerivedPolymarketCredentials(HttpClient http, ObjectMapper mapper,
                                        String privateKey, String address, String exchangeContract,
                                        String negRiskExchangeContract) {
        this.privateKey = privateKey;
        this.address = address;
        this.exchangeContract = exchangeContract;
        this.negRiskExchangeContract = negRiskExchangeContract;
        this.authClobApi = new PolymarketClobApi(http, mapper, this);
    }

    @Override
    public String apiKey() {
        return resolve().apiKey();
    }

    @Override
    public String apiSecret() {
        return resolve().secret();
    }

    @Override
    public String passphrase() {
        return resolve().passphrase();
    }

    @Override
    public String address() {
        return address;
    }

    @Override
    public String privateKey() {
        return privateKey;
    }

    @Override
    public String exchangeContract() {
        return exchangeContract;
    }

    @Override
    public String negRiskExchangeContract() {
        return negRiskExchangeContract;
    }

    private ApiKeyCredentials resolve() {
        ApiKeyCredentials existing = derived;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (derived == null) {
                derived = authClobApi.deriveApiKey(API_KEY_NONCE)
                        .onErrorResume(err -> authClobApi.createApiKey(API_KEY_NONCE))
                        .block();
            }
            return derived;
        }
    }
}

