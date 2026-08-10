package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest;

public interface PolymarketCredentials {
    String apiKey();

    String address();

    String passphrase();

    String apiSecret();

    String privateKey();

    String exchangeContract();

    String negRiskExchangeContract();
}

