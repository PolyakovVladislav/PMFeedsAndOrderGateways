package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.ws;

import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.ws.model.MarketEvent;
import reactor.core.publisher.Flux;

public interface PolymarketDataStream {
    Flux<MarketEvent> marketStream(String[] assetIds, boolean customFeatureEnabled);
}

