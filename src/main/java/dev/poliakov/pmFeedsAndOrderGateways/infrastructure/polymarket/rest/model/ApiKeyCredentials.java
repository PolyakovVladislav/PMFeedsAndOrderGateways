package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ApiKeyCredentials(String apiKey, String secret, String passphrase) {
}

