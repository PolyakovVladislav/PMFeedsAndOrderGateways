package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun;

import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.gateway.Eip712Utils;

public class EoaPredictFunCredentials implements PredictFunCredentials {

    private static final String CTF_EXCHANGE =
            "0x8BC070BEdAB741406F4B1Eb65A72bee27894B689";
    private static final String NEG_RISK_CTF_EXCHANGE =
            "0x365fb81bd4A24D6303cd2F19c349dE6894D8d58A";
    private static final String YIELD_BEARING_CTF_EXCHANGE =
            "0x6bEb5a40C032AFc305961162d8204CDA16DECFa5";
    private static final String YIELD_BEARING_NEG_RISK_CTF_EXCHANGE =
            "0x8A289d458f5a134bA40015085A8F50Ffb681B41d";

    private static final long BNB_MAINNET_CHAIN_ID = 56L;

    private final String apiKey;
    private final String privateKey;
    private final String address;
    private final long chainId;
    private final String ctfExchange;
    private final String negRiskCtfExchange;
    private final String yieldBearingCtfExchange;
    private final String yieldBearingNegRiskCtfExchange;

    public EoaPredictFunCredentials(String apiKey, String privateKey, long chainId,
                                    String ctfExchange, String negRiskCtfExchange,
                                    String yieldBearingCtfExchange,
                                    String yieldBearingNegRiskCtfExchange) {
        this.apiKey = apiKey == null ? "" : apiKey.trim();
        this.privateKey = privateKey == null ? "" : privateKey.trim();
        this.chainId = chainId <= 0 ? BNB_MAINNET_CHAIN_ID : chainId;
        this.ctfExchange = orDefault(ctfExchange, CTF_EXCHANGE);
        this.negRiskCtfExchange = orDefault(negRiskCtfExchange, NEG_RISK_CTF_EXCHANGE);
        this.yieldBearingCtfExchange = orDefault(yieldBearingCtfExchange, YIELD_BEARING_CTF_EXCHANGE);
        this.yieldBearingNegRiskCtfExchange =
                orDefault(yieldBearingNegRiskCtfExchange, YIELD_BEARING_NEG_RISK_CTF_EXCHANGE);

        this.address = this.privateKey.isBlank() ? "" : Eip712Utils.deriveAddress(this.privateKey);
    }

    private static String orDefault(String configured, String fallback) {
        return configured == null || configured.isBlank() ? fallback : configured.trim();
    }

    @Override
    public String apiKey() {
        return apiKey;
    }

    @Override
    public String privateKey() {
        return privateKey;
    }

    @Override
    public String address() {
        return address;
    }

    @Override
    public long chainId() {
        return chainId;
    }

    @Override
    public String exchangeContract(boolean negRisk, boolean yieldBearing) {
        if (negRisk) {
            return yieldBearing ? yieldBearingNegRiskCtfExchange : negRiskCtfExchange;
        }
        return yieldBearing ? yieldBearingCtfExchange : ctfExchange;
    }
}

