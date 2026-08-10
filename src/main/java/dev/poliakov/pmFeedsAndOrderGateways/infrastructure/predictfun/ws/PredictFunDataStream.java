package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.ws;

import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.ws.model.PredictFunMarketEvent;
import reactor.core.publisher.Flux;

public interface PredictFunDataStream {
    Flux<PredictFunMarketEvent> marketStream(long marketId);
}

