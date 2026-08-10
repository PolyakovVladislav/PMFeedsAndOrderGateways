package dev.poliakov.pmFeedsAndOrderGateways.client;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

// The client's own thread, and the reasoning for there being exactly one.
//
// ── main ──────────────────────────────────────────────────────────────────────────────────────
// One thread owns every venue's book parsing. Netty's event loops stay free to do nothing but read
// frames: each stream hops off the loop immediately, before any parsing, because that loop is
// shared across every connection in the process and parsing on it would delay all of them.
//
// One thread rather than one per venue, or one per token as Polymarket used to have. The number is
// not about capacity — a pessimistic 5000 book ticks/second at ~100µs each is about half a core —
// it is about handoffs. A basket's legs live on different tokens and different venues, so any N>1
// leaves a combo whose legs land on different threads needing a hop before it can be priced as a
// whole. Only at N=1 does that hop stop existing rather than merely shrinking.
//
// The rule that comes with it: nothing blocking may run here. One slow operation stalls market data
// for every venue at once. Order signing is the obvious offender and is kept off it — Polymarket's
// gateway hops to Schedulers.boundedElastic() before signing.
//
// ── daemon ────────────────────────────────────────────────────────────────────────────────────
// A daemon thread, which is where this deliberately differs from the same topology inside an
// application. A library must not keep a JVM alive because a consumer forgot to close a client:
// PredictionMarketClient.close() disposes it properly, and daemon makes forgetting a leak rather
// than a hang.
final class ClientSchedulers implements AutoCloseable {

    private final Scheduler main = Schedulers.newSingle("pm-main", true);

    Scheduler main() {
        return main;
    }

    @Override
    public void close() {
        main.dispose();
    }
}
