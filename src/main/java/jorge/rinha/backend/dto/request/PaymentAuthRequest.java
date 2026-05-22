package jorge.rinha.backend.dto.request;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public record PaymentAuthRequest(

        @JsonProperty("id")
        String id, // identificador único da transação

        @JsonProperty("transaction")
        Transaction transaction, // dados da transação

        @JsonProperty("customer")
        Customer customer, // dados do cliente

        @JsonProperty("merchant")
        Merchant merchant, // dados do estabelecimento

        @JsonProperty("terminal")
        Terminal terminal, // dados do terminal de pagamento

        @JsonProperty("last_transaction")
        LastTransaction lastTransaction // última transação do cliente (pode ser nulo)

) {

        public record Transaction(
                @JsonProperty("amount")
                float amount, // valor da transação

                @JsonProperty("installments")
                int installments, // número de parcelas

                @JsonProperty("requested_at")
                String requestedAt // data e hora da transação em UTC
        ) {}

        public record Customer(
                @JsonProperty("avg_amount")
                float avgAmount, // valor médio das transações do cliente

                @JsonProperty("tx_count_24h")
                int txCount24h, // quantidade de transações nas últimas 24 horas

                @JsonProperty("known_merchants")
                List<String> knownMerchants // lista de estabelecimentos conhecidos pelo cliente
        ) {}

        public record Merchant(
                @JsonProperty("id")
                String id, // identificador do estabelecimento

                @JsonProperty("mcc")
                String mcc, // código de categoria do estabelecimento

                @JsonProperty("avg_amount")
                float avgAmount // valor médio das transações do estabelecimento
        ) {}

        public record Terminal(
                @JsonProperty("is_online")
                boolean isOnline, // indica se o terminal está online

                @JsonProperty("card_present")
                boolean cardPresent, // indica se o cartão está presente fisicamente

                @JsonProperty("km_from_home")
                float kmFromHome // distância em km do terminal até a residência do cliente
        ) {}

        public record LastTransaction(
                @JsonProperty("timestamp")
                String timestamp, //data/hora

                @JsonProperty("km_from_current")
                float KmFromCurrent
        ) {}

}


