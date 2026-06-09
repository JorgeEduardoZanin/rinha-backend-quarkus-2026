package jorge.rinha.backend.service;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.buffer.Buffer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jorge.rinha.backend.utils.Parser;
import jorge.rinha.backend.utils.Utils;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.GZIPInputStream;

@ApplicationScoped
public class PaymentAuthService {

    /**
     * Parser manual responsável por extrair os campos necessários do JSON da requisição.
     *
     * <p>É usado no caminho crítico de performance para evitar desserialização completa
     * com ObjectMapper/DTO durante o processamento da API.</p>
     */
    private final Parser parser;

    public PaymentAuthService(Parser parser) {
        this.parser = parser;
    }

    private volatile boolean ready = false;
    private static final int CANDIDATE_BUCKETS = 32;

    private static final float INV_23 = 1.0f / 23.0f;
    private static final float INV_6  = 1.0f / 6.0f;

    private static final float MAX_AMOUNT              = 10000.0f;
    private static final float MAX_MERCHANT_AVG_AMOUNT = 10000.0f;
    private static final float MAX_MINUTES             = 1440.0f;
    private static final float MAX_TX_COUNT_24H        = 20.0f;
    private static final float MAX_INSTALLMENTS        = 12.0f;
    private static final float AMOUNT_VS_AVG_RATIO     = 10.0f;
    private static final float MAX_KM                  = 1000.0f;

    private static final int NUM_BUCKETS   = 8192;
    private static final int VECTOR_SIZE   = 14;

    /**
     * Tamanho, em bytes, de um float no arquivo binário.
     */
    private static final int FLOAT_SIZE    = 4;

    /**
     * Tamanho total, em bytes, de cada registro no arquivo binário.
     *
     * <p>Cada registro contém 14 floats, 1 byte de label e 3 bytes de padding.</p>
     */
    private static final int REGISTER_SIZE = 60;

    /**
     * Offset, dentro de cada registro binário, onde fica o label da transação.
     *
     * <p>Como o vetor possui 14 floats e cada float possui 4 bytes, o label começa no byte 56.</p>
     */
    private static final int LABEL_OFFSET  = VECTOR_SIZE * FLOAT_SIZE; // 56

    /**
     * Offset inicial de cada bucket dentro do arquivo binário mapeado.
     *
     * <p>O array possui {@code NUM_BUCKETS + 1} posições para permitir calcular o fim de um bucket
     * usando {@code offsets[bucket + 1]}.</p>
     */
    private final int[] offsets = new int[NUM_BUCKETS + 1];

    /**
     * Array plano contendo os centróides dos buckets.
     *
     * <p>Cada bucket ocupa {@code VECTOR_SIZE} posições consecutivas.</p>
     */
    private final float[] centroids = new float[NUM_BUCKETS * VECTOR_SIZE];

    /**
     * Limite mínimo de cada dimensão para cada bucket.
     *
     * <p>Usado para calcular a distância mínima entre o vetor da requisição e a bounding box do bucket,
     * permitindo podar buckets que não podem melhorar o top-5 atual.</p>
     */
    private final float[] bboxMins  = new float[NUM_BUCKETS * VECTOR_SIZE];

    /**
     * Limite máximo de cada dimensão para cada bucket.
     *
     * <p>Usado junto com {@link #bboxMins} para a poda por bounding box.</p>
     */
    private final float[] bboxMaxs  = new float[NUM_BUCKETS * VECTOR_SIZE];

    /**
     * Variável usada para impedir que o compilador/runtime elimine o loop de warmup do mmap.
     */
    private static volatile byte WARMUP_SINK;

    /**
     * Quantidade total de vetores carregados no dataset.
     */
    private int totalVectors;
    private MappedByteBuffer buffer;


    public boolean isReady() {
        return ready;
    }

    /**
     * Estrutura reutilizável por thread para evitar alocações no caminho crítico da requisição.
     *
     * <p>Cada thread recebe sua própria instância via {@link ThreadLocal}, permitindo reaproveitar
     * arrays e objetos auxiliares sem concorrência entre requisições processadas em threads diferentes.</p>
     */
    private static final class Scratch {
        final Parser.ParsedRequest parsed = new Parser.ParsedRequest();

        final float[] vector = new float[VECTOR_SIZE];

        final int[] bestBuckets = new int[CANDIDATE_BUCKETS];
        final float[] bestDists = new float[CANDIDATE_BUCKETS];

        final float[] topDists = new float[5];
        final boolean[] topFraud = new boolean[5];

        final int[] seenBuckets = new int[NUM_BUCKETS];
        int generation = 1;
    }

    /**
     * Cache local por thread contendo buffers e objetos reutilizáveis durante o processamento.
     *
     * <p>Reduz pressão no GC e evita alocações repetidas no caminho crítico da API.</p>
     */
    private static final ThreadLocal<Scratch> SCRATCH =
            ThreadLocal.withInitial(Scratch::new);



    /**
     * Respostas JSON pré-montadas para cada quantidade possível de vizinhos fraudulentos no top-5.
     *
     * <p>O índice do array representa a quantidade de fraudes encontradas entre os 5 vizinhos mais
     * próximos. Isso evita montar strings JSON ou serializar objetos a cada requisição.</p>
     *
     * <ul>
     *     <li>0 fraudes: aprovado com score 0.0</li>
     *     <li>1 fraude: aprovado com score 0.2</li>
     *     <li>2 fraudes: aprovado com score 0.4</li>
     *     <li>3 fraudes: reprovado com score 0.6</li>
     *     <li>4 fraudes: reprovado com score 0.8</li>
     *     <li>5 fraudes: reprovado com score 1.0</li>
     * </ul>
     */
    public static final String[] RESULT_JSONS = {
            "{\"approved\":true,\"fraud_score\":0.0}",
            "{\"approved\":true,\"fraud_score\":0.2}",
            "{\"approved\":true,\"fraud_score\":0.4}",
            "{\"approved\":false,\"fraud_score\":0.6}",
            "{\"approved\":false,\"fraud_score\":0.8}",
            "{\"approved\":false,\"fraud_score\":1.0}"
    };


    /**
     * Inicializa o dataset e os metadados necessários para o cálculo de score de fraude.
     *
     * <p>Este método é executado automaticamente durante o startup do Quarkus por observar
     * o evento {@link StartupEvent}. O carregamento é feito em uma virtual thread para não
     * bloquear diretamente a thread de inicialização da aplicação.</p>
     *
     * <p>O comportamento é controlado por variáveis de ambiente:</p>
     *
     * <ul>
     *     <li>{@code DATA_BIN_PATH}: caminho do arquivo binário com os vetores agrupados.</li>
     *     <li>{@code CENTROIDS_BIN_PATH}: caminho do arquivo com centróides, offsets e bounding boxes.</li>
     *     <li>{@code LOAD_DATASET=true}: gera os arquivos do dataset antes de carregá-los.</li>
     *     <li>{@code GENERATE_DATASET_ONLY=true}: gera os arquivos e encerra o processo em seguida.</li>
     * </ul>
     *
     * <p>Fluxo normal:</p>
     *
     * <ol>
     *     <li>Resolve os caminhos dos arquivos binários.</li>
     *     <li>Gera o dataset ou espera os arquivos ficarem disponíveis.</li>
     *     <li>Carrega centróides, offsets e bounding boxes.</li>
     *     <li>Mapeia o arquivo de vetores com {@link MappedByteBuffer}.</li>
     *     <li>Executa warmup do mmap para reduzir page faults nas primeiras requisições.</li>
     *     <li>Marca o serviço como pronto através da flag {@code ready}.</li>
     * </ol>
     *
     * @param event evento de inicialização disparado pelo Quarkus
     */
    void onStart(@Observes StartupEvent event) {
        Thread.ofVirtual().start(() -> {
            try {
                long start = System.currentTimeMillis();

                String binPath       = System.getenv().getOrDefault("DATA_BIN_PATH",      "/data/data.bin");
                String centroidsPath = System.getenv().getOrDefault("CENTROIDS_BIN_PATH", "/data/data_centroids.bin");
                boolean loadDataset  = "true".equals(System.getenv("LOAD_DATASET"));
                boolean generateOnly = "true".equals(System.getenv("GENERATE_DATASET_ONLY"));

                if (generateOnly) {
                    generateDataset(binPath, centroidsPath);

                    long end = System.currentTimeMillis();
                    System.out.printf("Dataset prebuilt in %d ms%n", end - start);

                    System.exit(0);
                    return;
                }

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

                warmupMappedBuffer(this.buffer, binFile.length());

                channel.close();
                raf.close();

                long end = System.currentTimeMillis();
                System.out.printf("Dataset ready in %d ms%n", end - start);

                ready = true;

            } catch (Exception e) {
                throw new RuntimeException("Failed to load dataset", e);
            }
        });
    }

    /**
     * Aquece o arquivo mapeado em memória para reduzir page faults nas primeiras requisições.
     *
     * <p>O {@link MappedByteBuffer#load()} é usado como hint para a JVM/SO carregarem
     * as páginas do arquivo em memória. Em seguida, o método toca explicitamente uma posição
     * a cada 4096 bytes, que é o tamanho comum de uma página de memória no Linux.</p>
     *
     * <p>Esse acesso antecipado ajuda a evitar que as primeiras requisições reais paguem o custo
     * de carregar páginas do arquivo sob demanda, reduzindo risco de picos no p99 logo após o startup.</p>
     *
     * <p>O acumulador {@code acc} é gravado em {@code WARMUP_SINK} para impedir que o compilador
     * ou runtime eliminem o loop por considerá-lo sem efeito observável.</p>
     *
     * @param buffer arquivo binário mapeado em memória
     * @param size tamanho total, em bytes, do arquivo mapeado
     */
    private static void warmupMappedBuffer(MappedByteBuffer buffer, long size) {
        buffer.load(); // hint para o SO/JVM

        byte acc = 0;

        // toca uma vez por página
        for (long p = 0; p < size; p += 4096) {
            acc ^= buffer.get((int) p);
        }

        // garante tocar o último byte também
        if (size > 0) {
            acc ^= buffer.get((int) (size - 1));
        }

        WARMUP_SINK = acc;
    }

    /**
     * Gera os arquivos binários usados pelo índice aproximado de KNN.
     *
     * <p>Este método lê o arquivo {@code references.json.gz}, extrai os vetores e labels
     * do dataset de referência, treina os centróides via K-Means, agrupa os vetores por bucket
     * e grava os arquivos necessários para consulta em runtime.</p>
     *
     * <p>De forma resumida, cada vetor do dataset é comparado com todos os centróides usando
     * distância nas 14 dimensões. O índice do centróide mais próximo é salvo em
     * {@code assignments[i]}, indicando em qual bucket aquele vetor será armazenado.</p>
     *
     * <p>Os centróides iniciais são selecionados de forma espaçada entre o início e o fim dos
     * vetores carregados, usando a posição proporcional de cada bucket dentro do total de registros.
     * Depois, esses centróides são refinados com K-Means para se aproximarem do centro real dos
     * vetores atribuídos a cada grupo.</p>
     *
     * <p>Ao final da geração, são criados dois arquivos binários:</p>
     *
     * <ul>
     *     <li>{@code data.bin}: contém os vetores agrupados por bucket, com seus respectivos labels.</li>
     *     <li>{@code data_centroids.bin}: contém centróides, offsets e bounding boxes de cada bucket.</li>
     * </ul>
     *
     * <p>O objetivo é transformar o dataset original em uma estrutura otimizada para consulta.
     * Em vez de varrer todos os vetores a cada requisição, a aplicação consulta apenas os buckets
     * mais próximos e faz scan somente nesses grupos.</p>
     *
     * <h3>Etapas principais</h3>
     *
     * <ol>
     *     <li>Carrega os vetores e labels do {@code references.json.gz} em arrays planos.</li>
     *     <li>Inicializa os centróides com sementes espalhadas pelo dataset inteiro.</li>
     *     <li>Executa treino K-Means usando uma amostra proporcional ao número de buckets.</li>
     *     <li>Executa um refinamento completo usando todos os vetores do dataset.</li>
     *     <li>Faz o assignment final de cada vetor para seu bucket mais próximo.</li>
     *     <li>Calcula offsets e bounding boxes dos buckets.</li>
     *     <li>Grava os arquivos binários usados no startup/runtime da aplicação.</li>
     * </ol>
     *
     * <p>O refinamento completo é a etapa mais importante para buckets maiores, pois corrige
     * centróides treinados inicialmente por amostra. Isso reduz o risco de fragmentar vizinhos
     * relevantes em buckets ruins, melhorando a qualidade da busca aproximada sem alterar a lógica
     * de decisão do KNN.</p>
     *
     * <p>A escrita do arquivo de vetores é feita por agrupamento linear usando prefix sum, evitando
     * o custo de percorrer todos os vetores para cada bucket.</p>
     *
     * @param binPath caminho onde será gravado o arquivo binário com os vetores agrupados
     * @param centroidsPath caminho onde será gravado o arquivo binário com centróides, offsets e bounding boxes
     * @throws Exception se ocorrer erro ao ler o dataset, treinar o índice ou gravar os arquivos binários
     */
    private void generateDataset(String binPath, String centroidsPath) throws Exception {
        System.out.println("Generating dataset with refined K-Means...");

        InputStream is = getClass().getResourceAsStream("/references.json.gz");
        if (is == null) throw new RuntimeException("references.json.gz not found in classpath");

        GZIPInputStream gz = new GZIPInputStream(is);
        ObjectMapper mapper = new ObjectMapper();
        var parser = mapper.createParser(gz);
        parser.nextToken();

        int MAX_VECTORS = 3_000_000;
        float[] allVecs = new float[MAX_VECTORS * VECTOR_SIZE];
        boolean[] allLabels = new boolean[MAX_VECTORS];

        int count = 0;

        while (parser.nextToken() == JsonToken.START_OBJECT && count < MAX_VECTORS) {
            boolean isFraud = false;

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String field = parser.getCurrentName();
                parser.nextToken();

                if ("label".equals(field)) {
                    isFraud = "fraud".equals(parser.getText());
                } else if ("vector".equals(field)) {
                    int d = 0;

                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        allVecs[(count * VECTOR_SIZE) + d] = parser.getFloatValue();
                        d++;
                    }
                }
            }

            allLabels[count] = isFraud;
            count++;
        }

        parser.close();
        gz.close();

        this.totalVectors = count;

        System.out.println("Loaded " + count + " vectors into memory. Starting K-Means training...");

        // 1. Seeds iniciais espalhadas no dataset inteiro.
        for (int i = 0; i < NUM_BUCKETS; i++) {
            int seedIdx = (int) (((long) i * count) / NUM_BUCKETS);
            int cOff = i * VECTOR_SIZE;
            int vOff = seedIdx * VECTOR_SIZE;

            for (int d = 0; d < VECTOR_SIZE; d++) {
                centroids[cOff + d] = allVecs[vOff + d];
            }
        }

        // 2. Treina os centróides usando uma amostra representativa do dataset.
        //
        // Em vez de usar apenas os primeiros registros, a amostra é espalhada ao longo
        // de todo o dataset. Para cada vetor da amostra, o algoritmo encontra o
        // centróide mais próximo comparando as 14 dimensões.
        //
        // Depois, cada centróide é recalculado como a média dos vetores que ficaram
        // mais próximos dele naquela iteração. Isso move o centróide para uma posição
        // mais representativa do grupo que ele está tentando formar.
        int sampleSize = Math.min(count, Math.max(100_000, NUM_BUCKETS * 32));
        int iterations = 5;

        for (int iter = 0; iter < iterations; iter++) {
            float[] newCentroids = new float[NUM_BUCKETS * VECTOR_SIZE];
            int[] counts = new int[NUM_BUCKETS];

            // Amostra espalhada no dataset inteiro, não só nas primeiras 100k.
            int shift = iter * 9973;

            for (int s = 0; s < sampleSize; s++) {
                int i = (int) ((((long) s * count) / sampleSize + shift) % count);

                int bestC = 0;
                float bestDist = Float.MAX_VALUE;
                int vOff = i * VECTOR_SIZE;

                for (int c = 0; c < NUM_BUCKETS; c++) {
                    float dist = 0.0f;
                    int cOff = c * VECTOR_SIZE;

                    for (int d = 0; d < VECTOR_SIZE; d++) {
                        float diff = allVecs[vOff + d] - centroids[cOff + d];
                        dist += diff * diff;

                        if (dist >= bestDist) {
                            break;
                        }
                    }

                    if (dist < bestDist) {
                        bestDist = dist;
                        bestC = c;
                    }
                }

                counts[bestC]++;

                int bestOff = bestC * VECTOR_SIZE;
                for (int d = 0; d < VECTOR_SIZE; d++) {
                    newCentroids[bestOff + d] += allVecs[vOff + d];
                }
            }

            for (int c = 0; c < NUM_BUCKETS; c++) {
                int cCount = counts[c];

                if (cCount > 0) {
                    int cOff = c * VECTOR_SIZE;

                    for (int d = 0; d < VECTOR_SIZE; d++) {
                        centroids[cOff + d] = newCentroids[cOff + d] / cCount;
                    }
                }
            }

            System.out.println("K-Means sample iteration " + (iter + 1) + "/" + iterations + " done.");
        }

        // 3. Refinamento completo usando o dataset inteiro.
        //
        // Todos os vetores são comparados com todos os centróides já treinados.
        // Cada vetor é associado temporariamente ao centróide mais próximo, apenas para
        // recalcular a média real daquele centróide com base no dataset completo.
        //
        // Essa etapa melhora a posição dos centróides, principalmente quando há muitos buckets
        System.out.println("Full refinement pass...");

        float[] refinedCentroids = new float[NUM_BUCKETS * VECTOR_SIZE];
        int[] refinedCounts = new int[NUM_BUCKETS];

        for (int i = 0; i < count; i++) {
            int bestC = 0;
            float bestDist = Float.MAX_VALUE;
            int vOff = i * VECTOR_SIZE;

            for (int c = 0; c < NUM_BUCKETS; c++) {
                float dist = 0.0f;
                int cOff = c * VECTOR_SIZE;

                for (int d = 0; d < VECTOR_SIZE; d++) {
                    float diff = allVecs[vOff + d] - centroids[cOff + d];
                    dist += diff * diff;

                    if (dist >= bestDist) {
                        break;
                    }
                }

                if (dist < bestDist) {
                    bestDist = dist;
                    bestC = c;
                }
            }

            refinedCounts[bestC]++;

            int bestOff = bestC * VECTOR_SIZE;
            for (int d = 0; d < VECTOR_SIZE; d++) {
                refinedCentroids[bestOff + d] += allVecs[vOff + d];
            }
        }

        for (int c = 0; c < NUM_BUCKETS; c++) {
            int cCount = refinedCounts[c];

            if (cCount > 0) {
                int cOff = c * VECTOR_SIZE;

                for (int d = 0; d < VECTOR_SIZE; d++) {
                    centroids[cOff + d] = refinedCentroids[cOff + d] / cCount;
                }
            }
        }

        System.out.println("Full refinement done. Final assignment...");

        // 4. Assignment final usando os centróides refinados.
        int[] assignments = new int[count];
        int[] bucketCounts = new int[NUM_BUCKETS];

        for (int i = 0; i < count; i++) {
            int bestC = 0;
            float bestDist = Float.MAX_VALUE;
            int vOff = i * VECTOR_SIZE;

            for (int c = 0; c < NUM_BUCKETS; c++) {
                float dist = 0.0f;
                int cOff = c * VECTOR_SIZE;

                for (int d = 0; d < VECTOR_SIZE; d++) {
                    float diff = allVecs[vOff + d] - centroids[cOff + d];
                    dist += diff * diff;

                    if (dist >= bestDist) {
                        break;
                    }
                }

                if (dist < bestDist) {
                    bestDist = dist;
                    bestC = c;
                }
            }

            assignments[i] = bestC;
            bucketCounts[bestC]++;
        }

        // 5. Offsets + BBox.
        int[] offs = new int[NUM_BUCKETS + 1];
        int currentOff = 0;

        for (int b = 0; b < NUM_BUCKETS; b++) {
            offs[b] = currentOff;
            currentOff += bucketCounts[b] * REGISTER_SIZE;

            int bOff = b * VECTOR_SIZE;

            for (int d = 0; d < VECTOR_SIZE; d++) {
                bboxMins[bOff + d] = Float.MAX_VALUE;
                bboxMaxs[bOff + d] = -Float.MAX_VALUE;
            }
        }

        offs[NUM_BUCKETS] = currentOff;

        // 6. Escrita agrupada O(n), sem loop bucket * count.
        System.out.println("Writing grouped binary file...");

        int[] bucketStart = new int[NUM_BUCKETS + 1];

        for (int b = 0; b < NUM_BUCKETS; b++) {
            bucketStart[b + 1] = bucketStart[b] + bucketCounts[b];
        }

        int[] sortedIdx = new int[count];
        int[] writePos = bucketStart.clone();

        for (int i = 0; i < count; i++) {
            int b = assignments[i];
            sortedIdx[writePos[b]++] = i;
        }

        DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(binPath), 1024 * 1024)
        );

        for (int b = 0; b < NUM_BUCKETS; b++) {
            int bOff = b * VECTOR_SIZE;
            int from = bucketStart[b];
            int to = bucketStart[b + 1];

            for (int k = from; k < to; k++) {
                int i = sortedIdx[k];
                int vOff = i * VECTOR_SIZE;

                for (int d = 0; d < VECTOR_SIZE; d++) {
                    float val = allVecs[vOff + d];
                    dos.writeFloat(val);

                    int idx = bOff + d;

                    if (val < bboxMins[idx]) bboxMins[idx] = val;
                    if (val > bboxMaxs[idx]) bboxMaxs[idx] = val;
                }

                dos.writeBoolean(allLabels[i]);

                dos.writeByte(0);
                dos.writeByte(0);
                dos.writeByte(0);
            }
        }

        dos.flush();
        dos.close();

        // 7. Escreve centróides + offsets + BBox.
        DataOutputStream dc = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(centroidsPath), 1024 * 1024)
        );

        dc.writeInt(NUM_BUCKETS);
        dc.writeInt(VECTOR_SIZE);

        for (int i = 0; i < centroids.length; i++) {
            dc.writeFloat(centroids[i]);
        }

        for (int off : offs) {
            dc.writeInt(off);
        }

        for (int i = 0; i < bboxMins.length; i++) {
            dc.writeFloat(bboxMins[i]);
        }

        for (int i = 0; i < bboxMaxs.length; i++) {
            dc.writeFloat(bboxMaxs[i]);
        }

        dc.flush();
        dc.close();

        System.out.println("Dataset generated completely with refined centroids.");
    }


    /**
     * Aguarda os arquivos binários do dataset ficarem disponíveis no disco.
     *
     * <p>No modelo atual, o dataset é gerado durante o build da imagem Docker e já é
     * copiado pronto para a imagem final. Por isso, este método não é usado no fluxo
     * normal de execução.</p>
     *
     * <p>Foi mantido apenas como alternativa para cenários futuros em que uma instância
     * gere o dataset em um volume compartilhado e outra instância precise aguardar
     * esses arquivos antes de carregar o modelo.</p>
     *
     * @param binPath caminho do arquivo binário com os vetores agrupados por bucket
     * @param centroidsPath caminho do arquivo binário com centróides, offsets e bounding boxes
     * @throws Exception se a thread for interrompida durante a espera
     */
    @Deprecated
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

    /**
     * Carrega em memória os metadados do índice usado para acelerar a busca KNN.
     *
     * <p>Este método lê o arquivo binário de centróides, offsets e bounding boxes
     * gerado previamente na etapa de criação do dataset. Esses dados são necessários
     * para que, durante uma requisição, a aplicação consiga localizar rapidamente os
     * buckets mais próximos do vetor recebido.</p>
     *
     * <p>O arquivo é lido exatamente na mesma ordem em que foi gravado:</p>
     *
     * <ol>
     *     <li>{@code numBuckets}: quantidade de buckets usada na geração do índice.</li>
     *     <li>{@code vectorSize}: quantidade de dimensões de cada vetor.</li>
     *     <li>{@code centroids}: centróides de todos os buckets.</li>
     *     <li>{@code offsets}: posição inicial de cada bucket dentro do arquivo {@code data.bin}.</li>
     *     <li>{@code bboxMins}: menor valor encontrado em cada dimensão de cada bucket.</li>
     *     <li>{@code bboxMaxs}: maior valor encontrado em cada dimensão de cada bucket.</li>
     * </ol>
     *
     * <p>A validação de {@code NUM_BUCKETS} e {@code VECTOR_SIZE} garante que o arquivo
     * carregado foi gerado com a mesma configuração do código atual. Caso esses valores
     * sejam diferentes, os arrays não bateriam com o layout esperado e o cálculo do score
     * poderia acessar posições incorretas ou produzir resultados inválidos.</p>
     *
     * <p>Os centróides são usados para escolher os buckets candidatos mais próximos.
     * Os offsets são usados para saber onde cada bucket começa e termina no arquivo
     * de vetores. As bounding boxes são usadas para descartar buckets que não podem
     * melhorar o top-5 atual.</p>
     *
     * @param path caminho do arquivo binário contendo centróides, offsets e bounding boxes
     * @throws Exception se ocorrer erro ao abrir, validar ou ler o arquivo
     */
    private void loadCentroids(String path) throws Exception {
        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(path)));

        int numBuckets = dis.readInt();
        int vectorSize = dis.readInt();

        if (numBuckets != NUM_BUCKETS || vectorSize != VECTOR_SIZE) {
            throw new RuntimeException("Centroids mismatch");
        }

        for (int i = 0; i < centroids.length; i++) {
            centroids[i] = dis.readFloat();
        }

        for (int i = 0; i <= NUM_BUCKETS; i++) {
            offsets[i] = dis.readInt();
        }

        for (int i = 0; i < bboxMins.length; i++) {
            bboxMins[i] = dis.readFloat();
        }

        for (int i = 0; i < bboxMaxs.length; i++) {
            bboxMaxs[i] = dis.readFloat();
        }

        dis.close();
        System.out.printf("Centroids and Bounding Boxes loaded: %d buckets%n", numBuckets);
    }

    /**
     * Processa uma requisição de score de fraude usando busca aproximada por KNN.
     *
     * <p>O método recebe o corpo da requisição em formato {@link Buffer}, faz o parsing manual
     * dos campos necessários, monta um vetor normalizado de 14 dimensões e compara esse vetor
     * com os centróides do índice K-Means para encontrar os buckets mais promissores.</p>
     *
     * <p>Depois de selecionar os buckets candidatos, o método escaneia os vetores armazenados
     * nesses buckets, mantendo em memória apenas os 5 vizinhos mais próximos encontrados. A decisão
     * final é tomada pela quantidade de vizinhos fraudulentos no top-5:</p>
     *
     * <ul>
     *     <li>0, 1 ou 2 vizinhos fraudulentos: transação aprovada.</li>
     *     <li>3, 4 ou 5 vizinhos fraudulentos: transação reprovada.</li>
     * </ul>
     *
     * <p>Para reduzir custo de CPU e pressão no GC, o método reutiliza arrays armazenados em
     * {@link Scratch} via {@link ThreadLocal}. Isso evita alocações por requisição no caminho
     * crítico da API.</p>
     *
     * <p>O método também usa duas otimizações importantes:</p>
     *
     * <ol>
     *     <li>Poda antecipada no cálculo de distância contra os centróides, interrompendo a soma
     *     quando a distância parcial já é pior que o pior candidato atual.</li>
     *     <li>Poda por bounding box, descartando buckets que não podem melhorar o top-5 atual.</li>
     * </ol>
     *
     * <p>Quando o resultado inicial fica na zona de borda, ou seja, com 2 ou 3 vizinhos fraudulentos,
     * o método executa uma busca complementar nos demais buckets ainda não visitados, mantendo a mesma
     * regra de decisão. Isso reduz risco de erro sem alterar a lógica do KNN.</p>
     *
     * @param body corpo da requisição HTTP contendo os dados da transação
     * @return JSON pré-montado com o status de aprovação e o score de fraude
     */
    public String processor(Buffer body) {
        Scratch scratch = SCRATCH.get();

        int generation = scratch.generation++;
        if (scratch.generation == Integer.MAX_VALUE) {
            java.util.Arrays.fill(scratch.seenBuckets, 0);
            scratch.generation = 1;
            generation = scratch.generation++;
        }

        int[] seenBuckets = scratch.seenBuckets;

        Parser.ParsedRequest req = scratch.parsed;
        parser.parse(body, req);

        float[] vector = scratch.vector;
        int[] bestBuckets = scratch.bestBuckets;
        float[] bestDists = scratch.bestDists;
        float[] topDists = scratch.topDists;
        boolean[] topFraud = scratch.topFraud;

        float dim5 = 0.0f;

        if (req.hasLastTransaction) {
            long minutes = (req.requestedEpochSeconds - req.lastEpochSeconds) / 60;

            dim5 = minutes <= 0
                    ? 0.0f
                    : (minutes >= MAX_MINUTES ? 1.0f : minutes / MAX_MINUTES);
        }

        vector[0]  = Utils.clamp(req.amount / MAX_AMOUNT);
        vector[1]  = Utils.clamp(req.installments / MAX_INSTALLMENTS);
        vector[2]  = Utils.clamp((req.amount / req.customerAvgAmount) / AMOUNT_VS_AVG_RATIO);
        vector[3]  = req.hour / INV_23;
        vector[4]  = Utils.getDayOfWeek(req.year, req.month, req.day) / INV_6;
        vector[5]  = req.hasLastTransaction ? Utils.clamp(dim5) : -1.0f;
        vector[6]  = req.hasLastTransaction ? Utils.clamp(req.kmFromCurrent / MAX_KM) : -1.0f;
        vector[7]  = Utils.clamp(req.terminalKmFromHome / MAX_KM);
        vector[8]  = Utils.clamp(req.txCount24h / MAX_TX_COUNT_24H);
        vector[9]  = req.terminalOnline ? 1.0f : 0.0f;
        vector[10] = req.terminalCardPresent ? 1.0f : 0.0f;
        vector[11] = req.knownMerchant ? 0.0f : 1.0f;
        vector[12] = mccValue(req.merchantMcc);
        vector[13] = Utils.clamp(req.merchantAvgAmount / MAX_MERCHANT_AVG_AMOUNT);

        for (int i = 0; i < CANDIDATE_BUCKETS; i++) {
            bestDists[i] = Float.MAX_VALUE;
        }

        topDists[0] = Float.MAX_VALUE;
        topDists[1] = Float.MAX_VALUE;
        topDists[2] = Float.MAX_VALUE;
        topDists[3] = Float.MAX_VALUE;
        topDists[4] = Float.MAX_VALUE;

        topFraud[0] = false;
        topFraud[1] = false;
        topFraud[2] = false;
        topFraud[3] = false;
        topFraud[4] = false;

        for (int i = 0; i < NUM_BUCKETS; i++) {
            int c = i * VECTOR_SIZE;
            float worst = bestDists[CANDIDATE_BUCKETS - 1];
            float dist = 0.0f;
            float diff;

            diff = vector[0] - centroids[c];
            dist += diff * diff;
            if (dist < worst) {
                diff = vector[1] - centroids[c + 1];
                dist += diff * diff;
                if (dist < worst) {
                    diff = vector[2] - centroids[c + 2];
                    dist += diff * diff;
                    if (dist < worst) {
                        diff = vector[3] - centroids[c + 3];
                        dist += diff * diff;
                        if (dist < worst) {
                            diff = vector[4] - centroids[c + 4];
                            dist += diff * diff;
                            if (dist < worst) {
                                diff = vector[5] - centroids[c + 5];
                                dist += diff * diff;
                                if (dist < worst) {
                                    diff = vector[6] - centroids[c + 6];
                                    dist += diff * diff;
                                    if (dist < worst) {
                                        diff = vector[7] - centroids[c + 7];
                                        dist += diff * diff;
                                        if (dist < worst) {
                                            diff = vector[8] - centroids[c + 8];
                                            dist += diff * diff;
                                            if (dist < worst) {
                                                diff = vector[9] - centroids[c + 9];
                                                dist += diff * diff;
                                                if (dist < worst) {
                                                    diff = vector[10] - centroids[c + 10];
                                                    dist += diff * diff;
                                                    if (dist < worst) {
                                                        diff = vector[11] - centroids[c + 11];
                                                        dist += diff * diff;
                                                        if (dist < worst) {
                                                            diff = vector[12] - centroids[c + 12];
                                                            dist += diff * diff;
                                                            if (dist < worst) {
                                                                diff = vector[13] - centroids[c + 13];
                                                                dist += diff * diff;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (dist < bestDists[CANDIDATE_BUCKETS - 1]) {
                int pos = CANDIDATE_BUCKETS - 1;

                while (pos > 0 && dist < bestDists[pos - 1]) {
                    bestDists[pos] = bestDists[pos - 1];
                    bestBuckets[pos] = bestBuckets[pos - 1];
                    pos--;
                }

                bestDists[pos] = dist;
                bestBuckets[pos] = i;
            }
        }


        for (int b = 0; b < CANDIDATE_BUCKETS; b++) {
            int bucketIdx = bestBuckets[b];
            seenBuckets[bucketIdx] = generation;

            if (isOutsideBBox(bucketIdx, vector, topDists[4])) {
                continue;
            }

            scanBucketOptimized(bucketIdx, vector, topDists, topFraud);
        }

        int fraudCount = countFrauds(topFraud);

        if (fraudCount == 2 || fraudCount == 3) {
            for (int bucketIdx = 0; bucketIdx < NUM_BUCKETS; bucketIdx++) {
                if (seenBuckets[bucketIdx] == generation) {
                    continue;
                }

                if (isOutsideBBox(bucketIdx, vector, topDists[4])) {
                    continue;
                }

                scanBucketOptimized(bucketIdx, vector, topDists, topFraud);
            }

            fraudCount = countFrauds(topFraud);
        }

        return RESULT_JSONS[fraudCount];
    }

    /**
     * Conta quantos dos 5 vizinhos mais próximos foram classificados como fraude.
     *
     * <p>O array {@code topFraud} representa o label dos 5 menores vetores encontrados
     * durante a busca KNN. Cada posição indica se aquele vizinho é fraudulento ou não.</p>
     *
     */
    private int countFrauds(boolean[] topFraud) {
        int fraudCount = 0;

        if (topFraud[0]) fraudCount++;
        if (topFraud[1]) fraudCount++;
        if (topFraud[2]) fraudCount++;
        if (topFraud[3]) fraudCount++;
        if (topFraud[4]) fraudCount++;

        return fraudCount;
    }

    /**
     * Verifica se um bucket pode ser descartado usando a bounding box pré-calculada.
     *
     * <p>Cada bucket possui uma caixa delimitadora em 14 dimensões, representada pelos
     * arrays {@code bboxMins} e {@code bboxMaxs}. Esses arrays guardam, para cada dimensão,
     * o menor e o maior valor encontrados entre todos os vetores daquele bucket.</p>
     *
     * <p>Este método calcula a menor distância quadrática possível entre o vetor da
     * requisição e a bounding box do bucket. Se essa distância mínima já for maior ou
     * igual à pior distância presente no top-5 atual, então nenhum vetor dentro desse
     * bucket pode melhorar o resultado atual. Nesse caso, o bucket pode ser ignorado
     * sem precisar escanear seus vetores no {@code data.bin}.</p>
     *
     * <p>A distância é acumulada dimensão por dimensão. Se o valor da requisição está
     * dentro do intervalo {@code [min, max]} daquela dimensão, a contribuição para a
     * distância mínima é zero. Se está abaixo do mínimo ou acima do máximo, é somada
     * a distância até a borda mais próxima da caixa.</p>
     *
     * <p>O código é escrito de forma manual e sem loop porque o vetor possui tamanho
     * fixo de 14 dimensões. Isso reduz overhead no caminho crítico da requisição e
     * permite retornar mais cedo assim que a distância acumulada já não puder melhorar
     * o top-5.</p>
     *
     * @param b índice do bucket que será avaliado
     * @param v vetor normalizado da requisição
     * @param worstDist pior distância atualmente presente no top-5, normalmente {@code topDists[4]}
     * @return {@code true} se o bucket pode ser ignorado; {@code false} se ainda precisa ser escaneado
     */
    private boolean isOutsideBBox(int b, float[] v, float worstDist) {
        int o = b * VECTOR_SIZE;
        float dist = 0.0f;
        float value, min, max, diff;

        value = v[0]; min = bboxMins[o]; max = bboxMaxs[o];
        if (value < min) { diff = min - value; dist += diff * diff; if (dist >= worstDist) return true; }
        else if (value > max) { diff = value - max; dist += diff * diff; if (dist >= worstDist) return true; }

        value = v[1]; min = bboxMins[o + 1]; max = bboxMaxs[o + 1];
        if (value < min) { diff = min - value; dist += diff * diff; if (dist >= worstDist) return true; }
        else if (value > max) { diff = value - max; dist += diff * diff; if (dist >= worstDist) return true; }

        value = v[2]; min = bboxMins[o + 2]; max = bboxMaxs[o + 2];
        if (value < min) { diff = min - value; dist += diff * diff; if (dist >= worstDist) return true; }
        else if (value > max) { diff = value - max; dist += diff * diff; if (dist >= worstDist) return true; }

        value = v[3]; min = bboxMins[o + 3]; max = bboxMaxs[o + 3];
        if (value < min) { diff = min - value; dist += diff * diff; if (dist >= worstDist) return true; }
        else if (value > max) { diff = value - max; dist += diff * diff; if (dist >= worstDist) return true; }

        value = v[4]; min = bboxMins[o + 4]; max = bboxMaxs[o + 4];
        if (value < min) { diff = min - value; dist += diff * diff; if (dist >= worstDist) return true; }
        else if (value > max) { diff = value - max; dist += diff * diff; if (dist >= worstDist) return true; }

        value = v[5]; min = bboxMins[o + 5]; max = bboxMaxs[o + 5];
        if (value < min) { diff = min - value; dist += diff * diff; if (dist >= worstDist) return true; }
        else if (value > max) { diff = value - max; dist += diff * diff; if (dist >= worstDist) return true; }

        value = v[6]; min = bboxMins[o + 6]; max = bboxMaxs[o + 6];
        if (value < min) { diff = min - value; dist += diff * diff; if (dist >= worstDist) return true; }
        else if (value > max) { diff = value - max; dist += diff * diff; if (dist >= worstDist) return true; }

        value = v[7]; min = bboxMins[o + 7]; max = bboxMaxs[o + 7];
        if (value < min) { diff = min - value; dist += diff * diff; if (dist >= worstDist) return true; }
        else if (value > max) { diff = value - max; dist += diff * diff; if (dist >= worstDist) return true; }

        value = v[8]; min = bboxMins[o + 8]; max = bboxMaxs[o + 8];
        if (value < min) { diff = min - value; dist += diff * diff; if (dist >= worstDist) return true; }
        else if (value > max) { diff = value - max; dist += diff * diff; if (dist >= worstDist) return true; }

        value = v[9]; min = bboxMins[o + 9]; max = bboxMaxs[o + 9];
        if (value < min) { diff = min - value; dist += diff * diff; if (dist >= worstDist) return true; }
        else if (value > max) { diff = value - max; dist += diff * diff; if (dist >= worstDist) return true; }

        value = v[10]; min = bboxMins[o + 10]; max = bboxMaxs[o + 10];
        if (value < min) { diff = min - value; dist += diff * diff; if (dist >= worstDist) return true; }
        else if (value > max) { diff = value - max; dist += diff * diff; if (dist >= worstDist) return true; }

        value = v[11]; min = bboxMins[o + 11]; max = bboxMaxs[o + 11];
        if (value < min) { diff = min - value; dist += diff * diff; if (dist >= worstDist) return true; }
        else if (value > max) { diff = value - max; dist += diff * diff; if (dist >= worstDist) return true; }

        value = v[12]; min = bboxMins[o + 12]; max = bboxMaxs[o + 12];
        if (value < min) { diff = min - value; dist += diff * diff; if (dist >= worstDist) return true; }
        else if (value > max) { diff = value - max; dist += diff * diff; if (dist >= worstDist) return true; }

        value = v[13]; min = bboxMins[o + 13]; max = bboxMaxs[o + 13];
        if (value < min) { diff = min - value; dist += diff * diff; return dist >= worstDist; }
        else if (value > max) { diff = value - max; dist += diff * diff; return dist >= worstDist; }

        return false;
    }

    /**
     * Escaneia todos os vetores de um bucket e atualiza o top-5 vizinhos mais próximos.
     *
     * <p>Este método percorre os registros do bucket informado diretamente no
     * {@link MappedByteBuffer}. Cada registro possui tamanho fixo de {@code REGISTER_SIZE}
     * bytes, sendo 14 valores {@code float} representando o vetor normalizado e 1 byte
     * final indicando se o registro é fraude ou legítimo.</p>
     *
     * <p>Os limites do bucket são obtidos pelo array {@code offsets}. O índice
     * {@code offsets[bucketIdx]} indica o início do bucket no arquivo {@code data.bin},
     * enquanto {@code offsets[bucketIdx + 1]} indica o fim. Dessa forma, o método consegue
     * percorrer apenas os vetores daquele bucket específico.</p>
     *
     * <p>Para cada vetor, é calculada a distância quadrática em relação ao vetor da
     * requisição. O cálculo é interrompido antecipadamente sempre que a distância parcial
     * já fica maior ou igual à pior distância presente no top-5 atual. Isso evita calcular
     * todas as 14 dimensões de vetores que claramente não podem melhorar o resultado.</p>
     *
     * <p>Quando a distância final de um vetor é menor que {@code topDists[4]}, o vetor
     * entra no top-5. O label de fraude é lido do último byte do registro e o par
     * distância/label é inserido em ordem usando {@code insertInOrder}.</p>
     *
     * @param bucketIdx índice do bucket que será escaneado
     * @param v vetor normalizado da requisição
     * @param topDists array com as 5 menores distâncias encontradas até o momento
     * @param topFraud array indicando se cada um dos 5 vizinhos atuais é fraude
     */
    private void scanBucketOptimized(int bucketIdx, float[] v, float[] topDists, boolean[] topFraud) {
        int pos = offsets[bucketIdx];
        int end = offsets[bucketIdx + 1];

        while (pos <= end - REGISTER_SIZE) {
            float worst = topDists[4];
            float dist = 0.0f;
            float diff;

            diff = v[0] - buffer.getFloat(pos);
            dist += diff * diff;
            if (dist >= worst) {
                pos += REGISTER_SIZE;
                continue;
            }

            diff = v[1] - buffer.getFloat(pos + 4);
            dist += diff * diff;
            if (dist >= worst) {
                pos += REGISTER_SIZE;
                continue;
            }

            diff = v[2] - buffer.getFloat(pos + 8);
            dist += diff * diff;
            if (dist >= worst) {
                pos += REGISTER_SIZE;
                continue;
            }

            diff = v[3] - buffer.getFloat(pos + 12);
            dist += diff * diff;
            if (dist >= worst) {
                pos += REGISTER_SIZE;
                continue;
            }

            diff = v[4] - buffer.getFloat(pos + 16);
            dist += diff * diff;
            if (dist >= worst) {
                pos += REGISTER_SIZE;
                continue;
            }

            diff = v[5] - buffer.getFloat(pos + 20);
            dist += diff * diff;
            if (dist >= worst) {
                pos += REGISTER_SIZE;
                continue;
            }

            diff = v[6] - buffer.getFloat(pos + 24);
            dist += diff * diff;
            if (dist >= worst) {
                pos += REGISTER_SIZE;
                continue;
            }

            diff = v[7] - buffer.getFloat(pos + 28);
            dist += diff * diff;
            if (dist >= worst) {
                pos += REGISTER_SIZE;
                continue;
            }

            diff = v[8] - buffer.getFloat(pos + 32);
            dist += diff * diff;
            if (dist >= worst) {
                pos += REGISTER_SIZE;
                continue;
            }

            diff = v[9] - buffer.getFloat(pos + 36);
            dist += diff * diff;
            if (dist >= worst) {
                pos += REGISTER_SIZE;
                continue;
            }

            diff = v[10] - buffer.getFloat(pos + 40);
            dist += diff * diff;
            if (dist >= worst) {
                pos += REGISTER_SIZE;
                continue;
            }

            diff = v[11] - buffer.getFloat(pos + 44);
            dist += diff * diff;
            if (dist >= worst) {
                pos += REGISTER_SIZE;
                continue;
            }

            diff = v[12] - buffer.getFloat(pos + 48);
            dist += diff * diff;
            if (dist >= worst) {
                pos += REGISTER_SIZE;
                continue;
            }

            diff = v[13] - buffer.getFloat(pos + 52);
            dist += diff * diff;

            if (dist < topDists[4]) {
                boolean isFraud = buffer.get(pos + LABEL_OFFSET) != 0;
                insertInOrder(topDists, topFraud, dist, isFraud);
            }

            pos += REGISTER_SIZE;
        }
    }

    /**
     * Insere um novo vizinho no top-5 mantendo as distâncias ordenadas.
     *
     * <p>O array {@code dists} armazena as 5 menores distâncias encontradas até o
     * momento durante a busca KNN. O array {@code frauds} armazena, na mesma posição,
     * se o respectivo vizinho é fraude ou legítimo.</p>
     *
     * <p>Este método assume que {@code dist} já é menor que a pior distância atual,
     * normalmente {@code dists[4]}. Por isso, o novo item deve obrigatoriamente entrar
     * no top-5, substituindo o pior vizinho ou sendo inserido em uma posição melhor.</p>
     *
     * <p>A inserção é feita de trás para frente. Enquanto a nova distância for menor
     * que a distância anterior, os valores existentes são deslocados uma posição para
     * a direita. Ao encontrar a posição correta, a nova distância e o respectivo label
     * de fraude são gravados juntos.</p>
     *
     * <p>Como o top-k é fixo em 5 posições, o método evita estruturas mais pesadas,
     * como fila de prioridade ou lista dinâmica, reduzindo alocações e overhead no
     * caminho crítico da requisição.</p>
     *
     * @param dists array ordenado com as 5 menores distâncias encontradas
     * @param frauds array paralelo indicando se cada vizinho correspondente é fraude
     * @param dist distância do novo vetor candidato
     * @param isFraud indica se o novo vetor candidato é fraude
     */
    private void insertInOrder(float[] dists, boolean[] frauds, float dist, boolean isFraud) {
        int i = 4;
        while (i > 0 && dist < dists[i - 1]) {
            dists[i] = dists[i - 1];
            frauds[i] = frauds[i - 1];
            i--;
        }
        dists[i] = dist;
        frauds[i] = isFraud;
    }

    /**
     * Converte o código MCC do estabelecimento em um valor numérico normalizado.
     *
     * <p>O MCC é uma categoria do merchant, como mercado, restaurante, farmácia,
     * apostas, companhia aérea, loja de departamento, entre outros. Como o modelo
     * KNN trabalha com vetores numéricos, o código bruto do MCC não é usado diretamente.
     * Em vez disso, cada MCC conhecido é convertido para um valor {@code float} dentro
     * de uma escala reduzida.</p>
     *
     * <p>Esse valor é usado como uma das 14 dimensões do vetor de comparação. MCCs com
     * valores próximos tendem a ficar mais próximos nessa dimensão; MCCs com valores
     * mais distantes contribuem mais para a distância final entre os vetores.</p>
     *
     * <p>Códigos não mapeados recebem o valor padrão {@code 0.50f}, funcionando como
     * uma categoria neutra/intermediária.</p>
     *
     * <p>Importante: qualquer alteração nesses valores muda a representação vetorial
     * do dataset. Portanto, se esse mapeamento for alterado, os arquivos binários do
     * dataset e dos centróides devem ser gerados novamente.</p>
     *
     * @param code código MCC do merchant
     * @return valor numérico normalizado usado na dimensão de MCC do vetor
     */
    private static float mccValue(int code) {
        return switch (code) {
            case 5411 -> 0.15f;
            case 5812 -> 0.30f;
            case 5912 -> 0.20f;
            case 5944 -> 0.45f;
            case 7801 -> 0.80f;
            case 7802 -> 0.75f;
            case 7995 -> 0.85f;
            case 4511 -> 0.35f;
            case 5311 -> 0.25f;
            case 5999 -> 0.50f;
            default   -> 0.50f;
        };
    }




}