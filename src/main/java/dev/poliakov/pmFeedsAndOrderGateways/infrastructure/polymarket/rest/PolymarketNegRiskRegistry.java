package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PolymarketNegRiskRegistry {

    private static final Logger log = LoggerFactory.getLogger(PolymarketNegRiskRegistry.class);

    private static final int LOOKUP_CONCURRENCY = 8;

    private final PolymarketClobApi clob;
    private final Map<String, Boolean> cache = new ConcurrentHashMap<>();

    public PolymarketNegRiskRegistry(PolymarketClobApi clob) {
        this.clob = clob;
    }

    public Mono<Map<String, Boolean>> resolve(Collection<String> assetIds) {
        List<String> missing = assetIds.stream()
                .distinct()
                .filter(id -> !cache.containsKey(id))
                .toList();
        if (missing.isEmpty()) {
            return Mono.fromCallable(() -> snapshot(assetIds));
        }
        return Flux.fromIterable(missing)
                .flatMap(assetId -> clob.fetchNegRisk(assetId)
                        .doOnNext(negRisk -> cache.put(assetId, negRisk)), LOOKUP_CONCURRENCY)
                .then(Mono.fromCallable(() -> snapshot(assetIds)));
    }

    public Mono<Void> prefetch(Collection<String> assetIds) {
        if (assetIds.isEmpty()) {
            return Mono.empty();
        }
        return resolve(assetIds)
                .doOnNext(flags -> log.info("Resolved Polymarket neg-risk flags: {}", flags))
                .doOnError(err -> log.warn("Could not prefetch neg-risk flags for {} — the first order "
                        + "on these tokens will resolve them inline", assetIds, err))
                .onErrorComplete()
                .then();
    }

    private Map<String, Boolean> snapshot(Collection<String> assetIds) {
        Map<String, Boolean> out = new HashMap<>();
        for (String assetId : assetIds) {
            Boolean negRisk = cache.get(assetId);
            if (negRisk == null) {
                throw new IllegalStateException("No neg-risk flag for Polymarket token " + assetId
                        + " — cannot tell which exchange contract to sign against, refusing to send "
                        + "an order that would be rejected as an invalid signature");
            }
            out.put(assetId, negRisk);
        }
        return out;
    }
}

