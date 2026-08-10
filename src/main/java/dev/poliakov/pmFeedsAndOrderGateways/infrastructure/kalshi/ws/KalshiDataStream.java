package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws;

import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi.ws.model.KalshiMarketEvent;
import reactor.core.publisher.Flux;

public interface KalshiDataStream {
    Flux<KalshiMarketEvent> marketStream(String marketTicker);
}

