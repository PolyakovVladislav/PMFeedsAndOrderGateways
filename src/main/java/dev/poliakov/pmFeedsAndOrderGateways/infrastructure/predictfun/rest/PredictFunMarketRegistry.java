package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest;

import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.Token;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.model.PredictFunMarketInfo;
import dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.model.PredictFunMarketResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PredictFunMarketRegistry {

    private static final Logger log = LoggerFactory.getLogger(PredictFunMarketRegistry.class);

    private static final int LOOKUP_CONCURRENCY = 8;

    private final PredictFunApi api;
    private final Map<Long, PredictFunMarketInfo> cache = new ConcurrentHashMap<>();

    public PredictFunMarketRegistry(PredictFunApi api) {
        this.api = api;
    }

    static PredictFunMarketInfo toInfo(long marketId, PredictFunMarketResponse.Market market) {
        Map<Token.Outcome, String> tokenIds = new LinkedHashMap<>();
        if (market.outcomes() != null) {
            for (PredictFunMarketResponse.Outcome outcome : market.outcomes()) {
                Token.Outcome mapped = parseOutcome(outcome.name());
                if (mapped != null && outcome.onChainId() != null && !outcome.onChainId().isBlank()) {
                    tokenIds.put(mapped, outcome.onChainId().trim());
                }
            }
        }
        if (tokenIds.isEmpty()) {
            throw new IllegalStateException("PredictFun market " + marketId + " reported no usable "
                    + "outcomes — refusing to build an order against it");
        }

        if (market.isNegRisk() == null || market.isYieldBearing() == null || market.feeRateBps() == null) {
            throw new IllegalStateException("PredictFun market " + marketId + " is missing one of "
                    + "isNegRisk/isYieldBearing/feeRateBps — cannot decide which contract signs an "
                    + "order or what fee it carries, refusing to guess");
        }
        return new PredictFunMarketInfo(marketId, market.isNegRisk(), market.isYieldBearing(),
                market.feeRateBps(), tokenIds);
    }

    private static Token.Outcome parseOutcome(String name) {
        if (name == null) {
            return null;
        }
        return switch (name.trim().toLowerCase(Locale.ROOT)) {
            case "yes" -> Token.Outcome.YES;
            case "no" -> Token.Outcome.NO;
            default -> null;
        };
    }

    public Mono<Map<Long, PredictFunMarketInfo>> resolve(Collection<Long> marketIds) {
        List<Long> missing = marketIds.stream()
                .distinct()
                .filter(id -> !cache.containsKey(id))
                .toList();
        if (missing.isEmpty()) {
            return Mono.fromCallable(() -> snapshot(marketIds));
        }
        return Flux.fromIterable(missing)
                .flatMap(marketId -> api.getMarket(marketId)
                        .map(market -> toInfo(marketId, market))
                        .doOnNext(info -> cache.put(marketId, info)), LOOKUP_CONCURRENCY)
                .then(Mono.fromCallable(() -> snapshot(marketIds)));
    }

    public Mono<Void> prefetch(Collection<Long> marketIds) {
        if (marketIds.isEmpty()) {
            return Mono.empty();
        }
        return resolve(marketIds)
                .doOnNext(resolved -> resolved.forEach((id, info) -> log.info(
                        "Resolved PredictFun market {}: negRisk={} yieldBearing={} feeRateBps={} outcomes={}",
                        id, info.negRisk(), info.yieldBearing(), info.feeRateBps(),
                        info.outcomeTokenIds().keySet())))
                .doOnError(err -> log.warn("Could not prefetch PredictFun markets {} — the first order "
                        + "on them will resolve inline", marketIds, err))
                .onErrorComplete()
                .then();
    }

    private Map<Long, PredictFunMarketInfo> snapshot(Collection<Long> marketIds) {
        Map<Long, PredictFunMarketInfo> out = new HashMap<>();
        for (Long marketId : marketIds) {
            PredictFunMarketInfo info = cache.get(marketId);
            if (info == null) {
                throw new IllegalStateException("No resolved metadata for PredictFun market " + marketId
                        + " — cannot tell which contract to sign against, which token id to name or "
                        + "what fee rate to carry, refusing to send an order that would be rejected");
            }
            out.put(marketId, info);
        }
        return out;
    }
}

