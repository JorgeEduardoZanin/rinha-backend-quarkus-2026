package jorge.rinha.backend.service;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jorge.rinha.backend.dto.request.PaymentAuthRequest;
import jorge.rinha.backend.dto.response.Result;
import jorge.rinha.backend.enums.MerchantCategoryCode;
import jorge.rinha.backend.utils.Utils;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.GZIPInputStream;

@ApplicationScoped
public class PaymentAuthService {

    public boolean validatorDataset = false;

    private static final float MAX_AMOUNT              = 10000.0f;
    private static final float MAX_MERCHANT_AVG_AMOUNT = 10000.0f;
    private static final float MAX_MINUTES             = 1440.0f;
    private static final float MAX_TX_COUNT_24H        = 20.0f;
    private static final float MAX_INSTALLMENTS        = 12.0f;
    private static final float AMOUNT_VS_AVG_RATIO     = 10.0f;
    private static final float MAX_KM                  = 1000.0f;

    private static final int NUM_BUCKETS   = 1024;
    private static final int VECTOR_SIZE   = 14;
    private static final int FLOAT_SIZE    = 4;
    private static final int REGISTER_SIZE = (VECTOR_SIZE * FLOAT_SIZE) + 1; // 57 bytes
    private static final int LSH_BITS = 6; // 2^6 = 64 baldes
    private static final int NUM_BUCKETS_LSH = 64;
    private final float[][] hyperplanes = new float[LSH_BITS][VECTOR_SIZE];

    private final long[]    offsets   = new long[NUM_BUCKETS + 1];
    private final float[][] centroids = new float[NUM_BUCKETS][VECTOR_SIZE];
    private int totalVectors;
    private MappedByteBuffer buffer;

    void onStart(@Observes StartupEvent event) throws Exception {
        long start = System.currentTimeMillis();

        String binPath       = System.getenv().getOrDefault("DATA_BIN_PATH",      "/data/data.bin");
        String centroidsPath = System.getenv().getOrDefault("CENTROIDS_BIN_PATH", "/data/data_centroids.bin");
        boolean loadDataset  = "true".equals(System.getenv("LOAD_DATASET"));

        if (loadDataset) {
            generateDataset(binPath, centroidsPath);
        } else {
            waitForDataset(binPath, centroidsPath);
        }

        loadCentroids(centroidsPath);

        File binFile = new File(binPath);
        RandomAccessFile raf = new RandomAccessFile(binFile, "r");
        FileChannel channel = raf.getChannel();
        this.buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, binFile.length());

        channel.close();
        raf.close();

        long end = System.currentTimeMillis();
        System.out.printf("Dataset ready in %d ms%n", end - start);

        validatorDataset = true;
    }
    private void generateDataset(String binPath, String centroidsPath) throws Exception {
        System.out.println("Generating dataset...");

        InputStream is = getClass().getResourceAsStream("/references.json.gz");
        if (is == null) throw new RuntimeException("references.json.gz not found in classpath");

        GZIPInputStream gz = new GZIPInputStream(is);
        DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(binPath))
        );

        ObjectMapper mapper = new ObjectMapper();
        var parser = mapper.createParser(gz);
        parser.nextToken();

        int count          = 0;
        int currentBucket  = 0;
        int bucketSize     = 3_000_000 / NUM_BUCKETS;
        float[] tmp        = new float[VECTOR_SIZE];
        float[][] centsSum = new float[NUM_BUCKETS][VECTOR_SIZE];
        int[] centsCnt     = new int[NUM_BUCKETS];
        long[] offs        = new long[NUM_BUCKETS + 1];

        while (parser.nextToken() == JsonToken.START_OBJECT) {
            boolean isFraud = false;

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String field = parser.getCurrentName();
                parser.nextToken();

                if ("label".equals(field)) {
                    isFraud = "fraud".equals(parser.getText());
                } else if ("vector".equals(field)) {
                    int idx = 0;
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        tmp[idx++] = parser.getFloatValue();
                    }
                }
            }

            if (count % bucketSize == 0 && currentBucket < NUM_BUCKETS) {
                offs[currentBucket] = (long) count * REGISTER_SIZE;
                currentBucket++;
            }

            int bucket = Math.min(currentBucket - 1, NUM_BUCKETS - 1);
            for (int d = 0; d < VECTOR_SIZE; d++) {
                centsSum[bucket][d] += tmp[d];
            }
            centsCnt[bucket]++;

            for (float f : tmp) dos.writeFloat(f);
            dos.writeBoolean(isFraud);
            count++;
        } // <- fecha o while aqui

        offs[NUM_BUCKETS] = (long) count * REGISTER_SIZE;
        this.totalVectors = count;

        dos.flush();
        dos.close();
        parser.close();
        gz.close();

        System.out.printf("data.bin generated: %d vectors%n", count);

        float[][] cents = new float[NUM_BUCKETS][VECTOR_SIZE];
        for (int b = 0; b < NUM_BUCKETS; b++) {
            int cnt = centsCnt[b];
            if (cnt == 0) continue;
            for (int d = 0; d < VECTOR_SIZE; d++) {
                cents[b][d] = centsSum[b][d] / cnt;
            }
        }

        DataOutputStream dc = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(centroidsPath))
        );
        dc.writeInt(NUM_BUCKETS);
        dc.writeInt(VECTOR_SIZE);
        for (int b = 0; b < NUM_BUCKETS; b++)
            for (int d = 0; d < VECTOR_SIZE; d++)
                dc.writeFloat(cents[b][d]);
        for (long off : offs) dc.writeLong(off);
        dc.flush();
        dc.close();

        System.out.println("data_centroids.bin generated");
    }

    private void waitForDataset(String binPath, String centroidsPath) throws Exception {
        System.out.println("Waiting for dataset from app1...");
        File bin       = new File(binPath);
        File centroids = new File(centroidsPath);

        while (!bin.exists() || !centroids.exists()) {
            Thread.sleep(500);
        }
        long lastSize = -1;
        while (true) {
            long currentSize = bin.length();
            if (currentSize == lastSize && currentSize > 0) break;
            lastSize = currentSize;
            Thread.sleep(1000);
        }
        System.out.println("Dataset found, mapping...");
    }

    private void loadCentroids(String path) throws Exception {
        DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(path))
        );

        int numBuckets = dis.readInt();
        int vectorSize = dis.readInt();

        if (numBuckets != NUM_BUCKETS || vectorSize != VECTOR_SIZE) {
            throw new RuntimeException(String.format(
                    "Centroids mismatch: expected %d/%d got %d/%d",
                    NUM_BUCKETS, VECTOR_SIZE, numBuckets, vectorSize
            ));
        }

        for (int b = 0; b < NUM_BUCKETS; b++)
            for (int d = 0; d < VECTOR_SIZE; d++)
                centroids[b][d] = dis.readFloat();

        for (int b = 0; b <= NUM_BUCKETS; b++)
            offsets[b] = dis.readLong();

        dis.close();
        System.out.printf("Centroids loaded: %d buckets x %d dims%n", numBuckets, vectorSize);
    }
    private int getBucketIdLSH(float[] vector) {
        int bucketId = 0;
        for (int i = 0; i < LSH_BITS; i++) {
            float dotProduct = 0.0f;
            for (int d = 0; d < VECTOR_SIZE; d++) {
                // Centraliza o dado em torno de 0 para o corte do hiperplano ser efetivo
                float val = vector[d] == -1.0f ? -0.5f : (vector[d] - 0.5f);
                dotProduct += val * hyperplanes[i][d];
            }
            if (dotProduct >= 0.0f) {
                bucketId |= (1 << i);
            }
        }
        return bucketId;
    }

    public Result processor(PaymentAuthRequest req) {
        String iso = req.transaction().requestedAt();

        float dim5 = 0.0f;
        if(req.lastTransaction() != null) {
            long epochCurrent = Utils.toEpochSeconds(req.transaction().requestedAt());
            long epochLast = Utils.toEpochSeconds(req.lastTransaction().timestamp());
            long minutes = (epochCurrent - epochLast) / 60;
            dim5 = minutes <= 0 ? 0.0f : (minutes >= MAX_MINUTES ? 1.0f : minutes / MAX_MINUTES);
        }
        //pego as datas da String manualmente, mais performatico que parsear para LocalDateTime ou OffsetDateTime e tirar hora, dia e ano.
        int hours = ((iso.charAt(11) - '0') * 10) + (iso.charAt(12) - '0');
        int year  = ((iso.charAt(0) - '0') * 1000) + ((iso.charAt(1) - '0') * 100) + ((iso.charAt(2) - '0') * 10)   + (iso.charAt(3) - '0');
        int month = ((iso.charAt(5) - '0') * 10)   + (iso.charAt(6) - '0');
        int day   = ((iso.charAt(8) - '0') * 10)   + (iso.charAt(9) - '0');

        //crio o vetor de 14 posicoes conforme o pedido. Colocando as variáveis globais como Float para evitar (float).
        float[] vector = new float[]{
                Utils.clamp(req.transaction().amount() / MAX_AMOUNT),
                Utils.clamp(req.transaction().installments() / MAX_INSTALLMENTS),
                Utils.clamp((req.transaction().amount() / req.customer().avgAmount()) / AMOUNT_VS_AVG_RATIO),
                hours / 23.0f,
                Utils.getDayOfWeek(year, month, day) / 6.0f,

                req.lastTransaction() == null ? -1.0f : Utils.clamp(dim5),
                req.lastTransaction() == null ? -1.0f : Utils.clamp(req.lastTransaction().KmFromCurrent() / MAX_KM),

                Utils.clamp(req.terminal().kmFromHome() / MAX_KM),
                Utils.clamp(req.customer().txCount24h() / MAX_TX_COUNT_24H),
                req.terminal().isOnline() ? 1.0f : 0.0f,
                req.terminal().cardPresent() ? 1.0f : 0.0f,
                req.customer().knownMerchants().contains(req.merchant().id()) ? 0.0f : 1.0f,
                MerchantCategoryCode.getValueByCode(req.merchant().mcc()),
                Utils.clamp(req.merchant().avgAmount() / MAX_MERCHANT_AVG_AMOUNT)
        };



        int[] bucketIndex = getThreeBestBuckets(vector);

        float score = calculateFraudScoreKNN(bucketIndex, vector, 5);


        return new Result(score < 0.6, score);
    }


    private int[] getThreeBestBuckets(float[] requestVector) {
        int b1 = 0, b2 = 0, b3 = 0;
        float d1 = Float.MAX_VALUE, d2 = Float.MAX_VALUE, d3 = Float.MAX_VALUE;

        for (int i = 0; i < NUM_BUCKETS; i++) {
            float dist = 0;
            float[] centroid = centroids[i];

            for (int j = 0; j < VECTOR_SIZE; j++) {
                float diff = requestVector[j] - centroid[j];
                dist += diff * diff;
                if (dist >= d3) break;
            }

            if (dist < d1) {
                d3 = d2; b3 = b2;
                d2 = d1; b2 = b1;
                d1 = dist; b1 = i;
            } else if (dist < d2) {
                d3 = d2; b3 = b2;
                d2 = dist; b2 = i;
            } else if (dist < d3) {
                d3 = dist; b3 = i;
            }
        }
        return new int[]{b1, b2, b3};
    }

    private static class Neighbor {
        float distance;
        boolean isFraud;
    }

    private float calculateFraudScoreKNN(int[] bucketIndices, float[] requestVector, int k) {
        Neighbor[] topK = new Neighbor[k];
        for (int i = 0; i < k; i++) {
            topK[i] = new Neighbor();
            topK[i].distance = Float.MAX_VALUE;
        }

        for (int bucketIdx : bucketIndices) {
            long startOffset = offsets[bucketIdx];
            long endOffset = offsets[bucketIdx + 1];
            long bufferLimit = buffer.capacity();

            if (endOffset > bufferLimit) {
                endOffset = bufferLimit;
            }

            for (long pos = startOffset; pos <= (endOffset - REGISTER_SIZE); pos += REGISTER_SIZE) {
                float currentDistSq = 0;

                for (int j = 0; j < VECTOR_SIZE; j++) {
                    int readPos = (int) (pos + (j * FLOAT_SIZE));
                    float val = buffer.getFloat(readPos);

                    float diff = requestVector[j] - val;
                    currentDistSq += diff * diff;

                    if (currentDistSq >= topK[k - 1].distance) {
                        // CORREÇÃO: joga o valor para o infinito antes de quebrar o loop,
                        // impedindo que a validação abaixo passe com um vetor incompleto.
                        currentDistSq = Float.MAX_VALUE;
                        break;
                    }
                }

                if (currentDistSq < topK[k - 1].distance) {
                    int booleanReadPos = (int) (pos + (VECTOR_SIZE * FLOAT_SIZE));
                    boolean isFraud = buffer.get(booleanReadPos) != 0;
                    insertInOrder(topK, currentDistSq, isFraud);
                }
            }
        }

        int fraudCount = 0;
        for (Neighbor n : topK) {
            if (n.isFraud) fraudCount++;
        }

        return (float) fraudCount / k;
    }

    private void insertInOrder(Neighbor[] topK, float dist, boolean isFraud) {
        int pos = topK.length - 1;
        while (pos > 0 && dist < topK[pos - 1].distance) {
            topK[pos].distance = topK[pos - 1].distance;
            topK[pos].isFraud = topK[pos - 1].isFraud;
            pos--;
        }
        topK[pos].distance = dist;
        topK[pos].isFraud = isFraud;
    }

}