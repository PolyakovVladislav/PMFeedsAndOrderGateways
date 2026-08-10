package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PredictFunAuthResponse(
        @JsonProperty("success") boolean success,
        @JsonProperty("data") Data data
) {

    public String message() {
        return data == null ? null : data.message();
    }

    public String token() {
        return data == null ? null : data.token();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Data(
            @JsonProperty("message") String message,
            @JsonProperty("token") String token
    ) {
    }
}

