package dev.poliakov.pmFeedsAndOrderGateways.domain.model.tradebot;

public final class TokenLabels {

    private TokenLabels() {
    }

    public static String shorten(String tokenId) {
        if (tokenId == null || tokenId.length() <= 12) {
            return tokenId;
        }
        return tokenId.substring(0, 5) + "…" + tokenId.substring(tokenId.length() - 6);
    }
}

