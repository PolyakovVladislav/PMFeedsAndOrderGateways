package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderResponse(
        boolean success,
        @JsonProperty("orderID") String orderId,
        String status,
        @JsonProperty("makingAmount") String makingAmount,
        @JsonProperty("takingAmount") String takingAmount,
        @JsonProperty("transactionsHashes") List<String> transactionHashes,
        @JsonProperty("tradeIDs") List<String> tradeIds,
        @JsonProperty("errorMsg") String errorMsg
) {
}

