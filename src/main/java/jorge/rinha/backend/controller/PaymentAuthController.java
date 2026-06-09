package jorge.rinha.backend.controller;

import io.quarkus.vertx.web.Route;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jorge.rinha.backend.service.PaymentAuthService;

/**
 * Controller responsável pelos endpoints HTTP da API de detecção de fraude.
 *
 * <p>Expõe a rota principal de cálculo de score de fraude e uma rota simples
 * de readiness para indicar se o dataset/modelo já foi carregado em memória.</p>
 */
@ApplicationScoped
public class PaymentAuthController {

    private static final String CONTENT_TYPE = "content-type";
    private static final String APPLICATION_JSON = "application/json";

    private final PaymentAuthService paymentAuthService;

    public PaymentAuthController(PaymentAuthService paymentAuthService) {
        this.paymentAuthService = paymentAuthService;
    }

    /**
     * Quarkus Vert.x Web, menos abstrações e mais controle.
     * Processa uma requisição de score de fraude.
     *
     * <p>Quando o serviço ainda não está pronto, retorna uma resposta fixa de fraude máxima.
     * Caso contrário, lê o corpo da requisição como {@link Buffer}, delega o processamento ao
     * {@link PaymentAuthService} e retorna o JSON gerado pelo serviço.</p>
     *
     * @param ctx contexto HTTP da requisição atual
     */
    @Route(path = "/fraud-score", methods = Route.HttpMethod.POST)
    void fraud(RoutingContext ctx) {
        if (!paymentAuthService.isReady()) {
            ctx.response()
                    .putHeader(CONTENT_TYPE, APPLICATION_JSON)
                    .end(PaymentAuthService.RESULT_JSONS[5]);
            return;
        }

        Buffer body = ctx.body().buffer();
        String result = paymentAuthService.processor(body);

        ctx.response()
                .putHeader(CONTENT_TYPE, APPLICATION_JSON)
                .end(result);
    }

    /**
     * Quarkus Vert.x Web, menos abstrações e mais controle.
     * Endpoint de readiness da aplicação.
     *
     * <p>Retorna HTTP 200 quando o serviço já terminou de carregar o dataset/modelo.
     * Enquanto o serviço ainda não estiver pronto, retorna HTTP 400.</p>
     *
     * @param ctx contexto HTTP da requisição atual
     */
    @Route(path = "/ready", methods = Route.HttpMethod.GET)
    void ready(RoutingContext ctx) {
        ctx.response()
                .setStatusCode(paymentAuthService.isReady() ? 200 : 400)
                .end();
    }
}