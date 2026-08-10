# PMFeedsAndOrderGateways

**Non-blocking market-data feeds and order placement for prediction markets — Polymarket, Kalshi and PredictFun — behind one small reactive API.**

![Java](https://img.shields.io/badge/Java-21-orange)
![Reactive](https://img.shields.io/badge/reactive-Project%20Reactor-blue)
![Tests](https://img.shields.io/badge/tests-30%20passing-brightgreen)
![No SDKs](https://img.shields.io/badge/exchange%20SDKs-0-lightgrey)
[![](https://jitpack.io/v/PolyakovVladislav/pm-feeds-and-order-gateways.svg)](https://jitpack.io/#PolyakovVladislav/pm-feeds-and-order-gateways)

Every venue speaks a different protocol — Polymarket signs EIP-712 orders over a CLOB, Kalshi signs RSA-PSS over REST, PredictFun signs EIP-712 on an EOA — and each streams its book differently. This library hides all of that behind **two ports** and normalises every book into one shape, so your code talks about *legs* and *orders*, not about any one exchange.

---

## Architecture

```mermaid
flowchart LR
    subgraph app["Your application"]
        CODE["trading / analytics logic"]
    end

    subgraph lib["PMFeedsAndOrderGateways"]
        CLIENT["PredictionMarketClient<br/>(builder)"]
        FEED{{"OrderBookFeed"}}
        GATE{{"OrderGateway"}}
        COMB["CombinedOrderBookFeed"]
        ROUTE["RoutingOrderGateway"]
    end

    subgraph venues["Venues"]
        POLY["Polymarket"]
        KAL["Kalshi"]
        PF["PredictFun"]
    end

    CODE -->|"credentials"| CLIENT
    CLIENT --> FEED
    CLIENT --> GATE
    FEED --- COMB
    GATE --- ROUTE

    POLY & KAL & PF ==>|"WebSocket · parsed on one thread"| COMB
    COMB ==>|"latest book per leg"| CODE

    CODE -->|"place / cancel"| ROUTE
    ROUTE -->|"sign off-thread · REST"| POLY & KAL & PF
```

Hexagonal by design: the two `domain.port.out` interfaces are the whole public surface. Which venue owns a leg is decided by the leg's `Token`, so a caller never selects an exchange by hand — the token does.

---

## Install

Consumed from [JitPack](https://jitpack.io). Add the repository and the dependency:

```kotlin
repositories {
    mavenCentral()
    maven { url = uri("https://jitpack.io") }
}

dependencies {
    implementation("com.github.PolyakovVladislav:PMFeedsAndOrderGateways:0.1.0")
}
```

> Available once the repository is pushed to GitHub and tagged `0.1.0` — JitPack builds it on first request.
> For local development, `./gradlew publishToMavenLocal` and depend on `dev.poliakov:pm-feeds-and-order-gateways:0.1.0` from `mavenLocal()`.

Requires **Java 21** (the code uses records, sealed interfaces and pattern-matching `switch`).

---

## Quick start

```java
import dev.poliakov.pmFeedsAndOrderGateways.client.PredictionMarketClient;
import dev.poliakov.pmFeedsAndOrderGateways.domain.model.order.*;
import dev.poliakov.pmFeedsAndOrderGateways.domain.port.out.*;

// You supply only what is yours — signing keys and account ids. Every public value
// (exchange contract addresses, chain id, REST/WS endpoints) is built in.
try (PredictionMarketClient client = PredictionMarketClient.builder()
        .polymarket("0xPRIVATE_KEY", "0xFUNDER_ADDRESS")
        .kalshi("api-key-id", "-----BEGIN PRIVATE KEY-----...")
        .build()) {

    OrderBookFeed feed    = client.orderBookFeed();
    OrderGateway  gateway = client.orderGateway();

    // Watch a cross-venue basket: a Polymarket leg and a Kalshi NO leg, together.
    List<Token> basket = List.of(
            new Token.Polymarket("71304094...."),
            new Token.Kalshi("KXHIGHNY-26AUG08-T89", Token.Outcome.NO));

    // Each emission carries the latest book for EVERY leg, best-priced-first.
    feed.combinedStream(basket).subscribe(books -> {
        // price the basket, decide whether to act
    });

    // Fill-and-kill: take what's there now, cancel the rest.
    Order order = new Order(basket.get(0), Order.Side.BUY, /*limit*/ 0.62, /*size*/ 10, /*bookPrice*/ 0.60);
    gateway.placeOrder(order, OrderType.FAK)
            .subscribe(result -> System.out.println(result.status() + " " + result.filledSize()));
}
```

Configure only the venues you use — a leg for an unconfigured venue fails loudly rather than silently. The client is `AutoCloseable`; `close()` releases the pooled REST connections and the parsing thread.

---

## The two ports

**`OrderBookFeed`** — live books for a set of legs, normalised across venues.

```java
Flux<List<TokenBookEvent>> combinedStream(List<Token> tokens);
```

Every emission carries the latest book for *all* legs (not just the one that ticked), so a basket prices from a single element. Books are best-priced-first; a Kalshi/PredictFun NO leg is the mirror of its YES book, computed for you. One-sided books are emitted rather than withheld — **check the side you need** (no asks → nothing to buy).

**`OrderGateway`** — venue-neutral placement, routed by token.

```java
Mono<OrderResult>        placeOrder(Order order, OrderType orderType);
Mono<List<OrderResult>>  placeOrders(List<Order> orders, OrderType orderType);   // one basket, one bulk request per venue
Mono<Double>             resolveFilledSize(Token token, String orderId);          // what a DELAYED order really filled
Mono<Void>               cancel(String orderId);
Mono<Void>               warmup();
```

**Placement across venues is concurrent, not atomic.** A basket spanning two exchanges is one bulk request per exchange; either can fill, partially fill or reject on its own. Always read the per-leg `OrderResult` and reconcile — an unhedged leg is a real position, not a retry.

---

## Supported venues

| Venue | Order signing | Market data |
|---|---|---|
| **Polymarket** | EIP-712 (secp256k1) over the CLOB | per-token WebSocket |
| **Kalshi** | RSA-PSS per REST request | one multiplexed WebSocket |
| **PredictFun** | EIP-712 on an EOA, 4 exchange contracts | WebSocket + JWT handshake |

---

## Design notes

- **No exchange SDKs.** Everything is hand-rolled on `reactor-netty` plus custom crypto (`web3j` for keccak/secp256k1, the JDK for RSA-PSS). Vendor SDKs were read as a *specification* of the wire format, never depended on.
- **One thread parses everything.** Netty's event loops do nothing but read frames; every venue's parsing hops onto a single daemon thread (`pm-main`). A basket's legs then price on the same thread that parsed them — no cross-thread handoff between a book update and the code that reads it. Order signing (the one expensive, blocking step) is explicitly pushed *off* that thread so it can never stall market data.
- **You give only what is yours.** Signing keys and account ids. Public infrastructure — contract addresses, chain id, endpoints — is built in, so a caller can't fat-finger a verifying contract.
- **Clean dependency surface.** `Reactor` is `api` (it's in every signature); `Jackson`, `web3j` and `slf4j` are `implementation` (internal only). The library binds no logger — that's the consumer's choice.

---

## Testing

```bash
./gradlew test
```

**30 tests, offline.** Credential derivation and socket opening are lazy, so the tests assemble the whole graph without touching the network: builder wiring, per-venue order arithmetic (including the cash-budgeted Polymarket BUY that guards a documented +47% over-buy), YES/NO book mirroring, one-sided book handling, and `DELAYED`-order resolution under virtual time.

---

## Status

`0.1.0` — extracted and self-contained, tests green. Known scope: `PredictFun` order cancellation is a stub (cancellation there is an on-chain action); `cancel()` routes by id shape and is best-effort. Not yet load-tested as a standalone dependency outside its origin project.
