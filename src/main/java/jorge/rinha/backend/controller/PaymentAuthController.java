package jorge.rinha.backend.controller;

import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jorge.rinha.backend.dto.request.PaymentAuthRequest;
import jorge.rinha.backend.dto.response.PaymentAuthResponse;
import jorge.rinha.backend.dto.response.Result;
import jorge.rinha.backend.service.PaymentAuthService;

@Path("/")
public class PaymentAuthController {


    private final PaymentAuthService paymentAuthService;

    public PaymentAuthController(PaymentAuthService paymentAuthService) {
        this.paymentAuthService = paymentAuthService;
    }

    @POST
    @Path("/fraud-score")
    //@RunOnVirtualThread
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Result processor(PaymentAuthRequest req){
        return paymentAuthService.processor(req);
    }

    @GET
    @Path("/ready")
    @RunOnVirtualThread
    public Response ready(){
        if(paymentAuthService.validatorDataset)return Response.status(200).entity(null).build();
        return Response.status(404).entity(null).build();
    }
}
