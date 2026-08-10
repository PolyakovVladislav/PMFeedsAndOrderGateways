package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi;

public interface KalshiCredentials {
    String apiKeyId();

    String sign(String message);
}

