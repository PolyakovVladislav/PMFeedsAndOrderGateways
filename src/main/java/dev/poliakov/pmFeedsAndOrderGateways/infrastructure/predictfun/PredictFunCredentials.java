package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun;

public interface PredictFunCredentials {

    String apiKey();

    String privateKey();

    String address();

    long chainId();

    String exchangeContract(boolean negRisk, boolean yieldBearing);
}

