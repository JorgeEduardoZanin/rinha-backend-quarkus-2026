package jorge.rinha.backend;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jorge.rinha.backend.dto.model.Reference;

import java.util.*;

@Path("/hello")
public class GreetingResource {

    static final double MAX_VALOR = 10000;
    static final double MAX_HORA  = 23;
    static final double MAX_MEDIA = 5000;

    record AmounReq(double valor, int hora, double mediaCliente){}
    record AmountRes(double valor, double hora, double mediaCliente){}

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Resultado hello(AmounReq req) {

        List<Reference> dataset = List.of(
                new Reference(new double[]{0.01, 0.08, 0.05}, false),
                new Reference(new double[]{0.97, 1.00, 1.00}, true),
                new Reference(new double[]{0.58, 0.92, 1.00}, true),
                new Reference(new double[]{0.40, 1.00, 1.00}, true),
                new Reference(new double[]{0.01, 0.08, 0.05}, false)
        );

        double[] consulta = {1.0, 0.96, 0.96};


        var  fraudScore = fraudScore(maisProxima(consulta, dataset, 3));
        var decidir = decidir(fraudScore, 0.6);
        IO.println(decidir.toString());
        return decidir;
    }

    public static double normalizador(double[] a, double[] b){

        double soma = 0;
        for(int i = 0; i < a.length; i++){
            double diff = a[i] - b[i];
            soma += diff * diff;
        }

        return Math.sqrt(soma);
    }

    public static List<Reference> maisProxima(double[] vetorConsulta, List<Reference> referencias, int k){

        List<double[]> list = new ArrayList<>();
        List<Reference> referencia = new ArrayList<>();
        double lastResponse = 0;
        for(int i = 0; i < referencias.size(); i++){
            double distancia = normalizador(vetorConsulta, referencias.get(i).vetor());
            list.add(new double[]{distancia, i});
        }

        list.sort(Comparator.comparingDouble(d -> d[0]));

        List<Reference> result = new ArrayList<>();
        for(int i = 0; i < k; i++){
            int idx = (int) list.get(i)[1];
            result.add(referencias.get(idx));
        }

        return result;
    }

    public static double fraudScore(List<Reference> referencias) {
        int contadorFraudes = 0;
        for (Reference v : referencias) {
            if (v.fraud()) contadorFraudes++;
        }

        return (double) contadorFraudes / referencias.size();
    }

    record Resultado(boolean aprovado, double fraudScore) {}


    public static Resultado decidir(double fraudScore, double threshold) {
        return new Resultado(fraudScore < threshold, fraudScore);
    }

}
