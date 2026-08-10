package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.model;

import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Token;

import java.util.Map;

public record PredictFunMarketInfo(
        long marketId,
        boolean negRisk,
        boolean yieldBearing,
        int feeRateBps,
        Map<Token.Outcome, String> outcomeTokenIds
) {

    public PredictFunMarketInfo {
        outcomeTokenIds = Map.copyOf(outcomeTokenIds);
    }

    public String tokenId(Token.Outcome outcome) {
        String tokenId = outcomeTokenIds.get(outcome);
        if (tokenId == null) {
            throw new IllegalStateException("PredictFun market " + marketId + " has no "
                    + outcome + " outcome — cannot build an order for a leg that does not exist");
        }
        return tokenId;
    }
}

