package jorge.rinha.backend.dto.response;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PaymentAuthResponse(
        @JsonProperty("approved") boolean approved,
        @JsonProperty("fraud_score") double fraudScore
){}
