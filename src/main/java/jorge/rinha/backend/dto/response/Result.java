package jorge.rinha.backend.dto.response;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Result (
        @JsonProperty("approved") boolean approved,
        @JsonProperty("fraud_score") float fraudScore
){}
