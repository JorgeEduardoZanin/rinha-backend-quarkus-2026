package jorge.rinha.backend.utils;

import io.vertx.core.buffer.Buffer;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;

@ApplicationScoped
public class Parser {

    private static final byte[] F_TRANSACTION       = bytes("\"transaction\"");
    private static final byte[] F_AMOUNT            = bytes("\"amount\"");
    private static final byte[] F_INSTALLMENTS      = bytes("\"installments\"");
    private static final byte[] F_REQUESTED_AT      = bytes("\"requested_at\"");

    private static final byte[] F_CUSTOMER          = bytes("\"customer\"");
    private static final byte[] F_AVG_AMOUNT        = bytes("\"avg_amount\"");
    private static final byte[] F_TX_COUNT_24H      = bytes("\"tx_count_24h\"");
    private static final byte[] F_KNOWN_MERCHANTS   = bytes("\"known_merchants\"");

    private static final byte[] F_MERCHANT          = bytes("\"merchant\"");
    private static final byte[] F_ID                = bytes("\"id\"");
    private static final byte[] F_MCC               = bytes("\"mcc\"");

    private static final byte[] F_TERMINAL          = bytes("\"terminal\"");
    private static final byte[] F_IS_ONLINE         = bytes("\"is_online\"");
    private static final byte[] F_CARD_PRESENT      = bytes("\"card_present\"");
    private static final byte[] F_KM_FROM_HOME      = bytes("\"km_from_home\"");

    private static final byte[] F_LAST_TRANSACTION  = bytes("\"last_transaction\"");
    private static final byte[] F_TIMESTAMP         = bytes("\"timestamp\"");
    private static final byte[] F_KM_FROM_CURRENT   = bytes("\"km_from_current\"");

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    public static final class ParsedRequest {
        public float amount;
        public int installments;

        public int year;
        public int month;
        public int day;
        public int hour;
        public long requestedEpochSeconds;

        public float customerAvgAmount;
        public int txCount24h;
        public boolean knownMerchant;

        public int merchantMcc;
        public float merchantAvgAmount;

        public boolean terminalOnline;
        public boolean terminalCardPresent;
        public float terminalKmFromHome;

        public boolean hasLastTransaction;
        public long lastEpochSeconds;
        public float kmFromCurrent;
    }

    public void parse(Buffer json, ParsedRequest out) {
        int transactionObj = valueStartOf(json, F_TRANSACTION, 0);

        int p = transactionObj;

        p = valueStartOf(json, F_AMOUNT, p);
        out.amount = parseFloat(json, p);

        p = valueStartOf(json, F_INSTALLMENTS, p);
        out.installments = parseInt(json, p);

        p = stringValueStartOf(json, F_REQUESTED_AT, p);
        parseRequestedAt(json, p, out);

        int customerObj = valueStartOf(json, F_CUSTOMER, p);

        p = customerObj;

        p = valueStartOf(json, F_AVG_AMOUNT, p);
        out.customerAvgAmount = parseFloat(json, p);

        p = valueStartOf(json, F_TX_COUNT_24H, p);
        out.txCount24h = parseInt(json, p);

        int knownMerchantsStart = valueStartOf(json, F_KNOWN_MERCHANTS, p);
        int knownMerchantsEnd = simpleArrayEnd(json, knownMerchantsStart);

        int merchantObj = valueStartOf(json, F_MERCHANT, knownMerchantsEnd);

        p = merchantObj;

        int merchantIdStart = stringValueStartOf(json, F_ID, p);
        int merchantIdEnd = stringEnd(json, merchantIdStart);

        out.knownMerchant = containsStringElementFast(
                json,
                knownMerchantsStart,
                knownMerchantsEnd,
                merchantIdStart,
                merchantIdEnd
        );

        p = merchantIdEnd;

        int mccStart = stringValueStartOf(json, F_MCC, p);
        out.merchantMcc = parseMcc4(json, mccStart);

        p = mccStart + 4;

        p = valueStartOf(json, F_AVG_AMOUNT, p);
        out.merchantAvgAmount = parseFloat(json, p);

        int terminalObj = valueStartOf(json, F_TERMINAL, p);

        p = terminalObj;

        p = valueStartOf(json, F_IS_ONLINE, p);
        out.terminalOnline = json.getByte(p) == 't';

        p = valueStartOf(json, F_CARD_PRESENT, p);
        out.terminalCardPresent = json.getByte(p) == 't';

        p = valueStartOf(json, F_KM_FROM_HOME, p);
        out.terminalKmFromHome = parseFloat(json, p);

        int lastTransactionValue = valueStartOf(json, F_LAST_TRANSACTION, p);

        if (json.getByte(lastTransactionValue) == 'n') {
            out.hasLastTransaction = false;
            out.lastEpochSeconds = 0L;
            out.kmFromCurrent = 0.0f;
            return;
        }

        out.hasLastTransaction = true;

        p = lastTransactionValue;

        int lastTimestampStart = stringValueStartOf(json, F_TIMESTAMP, p);
        out.lastEpochSeconds = parseEpochSecondsAt(json, lastTimestampStart);

        p = lastTimestampStart + 20;

        p = valueStartOf(json, F_KM_FROM_CURRENT, p);
        out.kmFromCurrent = parseFloat(json, p);
    }

    private static int valueStartOf(Buffer json, byte[] field, int from) {
        int fieldPos = indexOf(json, field, from);

        if (fieldPos < 0) {
            throw new IllegalArgumentException("Campo não encontrado");
        }

        int colon = indexOfByte(json, (byte) ':', fieldPos + field.length);

        return skipSpaces(json, colon + 1);
    }

    private static int stringValueStartOf(Buffer json, byte[] field, int from) {
        int valueStart = valueStartOf(json, field, from);
        return valueStart + 1;
    }

    private static int indexOf(Buffer json, byte[] target, int from) {
        int max = json.length() - target.length;

        outer:
        for (int i = from; i <= max; i++) {
            if (json.getByte(i) != target[0]) {
                continue;
            }

            for (int j = 1; j < target.length; j++) {
                if (json.getByte(i + j) != target[j]) {
                    continue outer;
                }
            }

            return i;
        }

        return -1;
    }

    private static int indexOfByte(Buffer json, byte target, int from) {
        int len = json.length();

        for (int i = from; i < len; i++) {
            if (json.getByte(i) == target) {
                return i;
            }
        }

        return -1;
    }

    private static int skipSpaces(Buffer json, int i) {
        while (true) {
            byte c = json.getByte(i);

            if (c != ' ' && c != '\n' && c != '\r' && c != '\t') {
                return i;
            }

            i++;
        }
    }

    private static int parseInt(Buffer json, int i) {
        int value = 0;

        while (true) {
            byte c = json.getByte(i);

            if (c < '0' || c > '9') {
                return value;
            }

            value = (value * 10) + (c - '0');
            i++;
        }
    }

    private static float parseFloat(Buffer json, int i) {
        boolean negative = false;

        if (json.getByte(i) == '-') {
            negative = true;
            i++;
        }

        float value = 0.0f;

        while (true) {
            byte c = json.getByte(i);

            if (c < '0' || c > '9') {
                break;
            }

            value = (value * 10.0f) + (c - '0');
            i++;
        }

        if (json.getByte(i) == '.') {
            i++;

            float factor = 0.1f;

            while (true) {
                byte c = json.getByte(i);

                if (c < '0' || c > '9') {
                    break;
                }

                value += (c - '0') * factor;
                factor *= 0.1f;
                i++;
            }
        }

        return negative ? -value : value;
    }

    private static int parseMcc4(Buffer json, int i) {
        return ((json.getByte(i) - '0') * 1000)
                + ((json.getByte(i + 1) - '0') * 100)
                + ((json.getByte(i + 2) - '0') * 10)
                +  (json.getByte(i + 3) - '0');
    }

    private static void parseRequestedAt(Buffer json, int start, ParsedRequest out) {
        int year = ((json.getByte(start) - '0') * 1000)
                + ((json.getByte(start + 1) - '0') * 100)
                + ((json.getByte(start + 2) - '0') * 10)
                +  (json.getByte(start + 3) - '0');

        int month = ((json.getByte(start + 5) - '0') * 10)
                +  (json.getByte(start + 6) - '0');

        int day = ((json.getByte(start + 8) - '0') * 10)
                +  (json.getByte(start + 9) - '0');

        int hour = ((json.getByte(start + 11) - '0') * 10)
                +  (json.getByte(start + 12) - '0');

        int minute = ((json.getByte(start + 14) - '0') * 10)
                +  (json.getByte(start + 15) - '0');

        int second = ((json.getByte(start + 17) - '0') * 10)
                +  (json.getByte(start + 18) - '0');

        out.year = year;
        out.month = month;
        out.day = day;
        out.hour = hour;
        out.requestedEpochSeconds = toEpochSeconds(year, month, day, hour, minute, second);
    }

    private static long parseEpochSecondsAt(Buffer json, int start) {
        int year = ((json.getByte(start) - '0') * 1000)
                + ((json.getByte(start + 1) - '0') * 100)
                + ((json.getByte(start + 2) - '0') * 10)
                +  (json.getByte(start + 3) - '0');

        int month = ((json.getByte(start + 5) - '0') * 10)
                +  (json.getByte(start + 6) - '0');

        int day = ((json.getByte(start + 8) - '0') * 10)
                +  (json.getByte(start + 9) - '0');

        int hour = ((json.getByte(start + 11) - '0') * 10)
                +  (json.getByte(start + 12) - '0');

        int minute = ((json.getByte(start + 14) - '0') * 10)
                +  (json.getByte(start + 15) - '0');

        int second = ((json.getByte(start + 17) - '0') * 10)
                +  (json.getByte(start + 18) - '0');

        return toEpochSeconds(year, month, day, hour, minute, second);
    }

    private static long toEpochSeconds(
            int year,
            int month,
            int day,
            int hour,
            int minute,
            int second
    ) {
        int y = year;
        int m = month;

        if (m <= 2) {
            y--;
            m += 12;
        }

        long days = 365L * y
                + (y / 4)
                - (y / 100)
                + (y / 400)
                + (306 * (m + 1) / 10)
                + day
                - 719591;

        return days * 86400L
                + hour * 3600L
                + minute * 60L
                + second;
    }

    private static int simpleArrayEnd(Buffer json, int start) {
        int i = start;

        while (json.getByte(i) != ']') {
            i++;
        }

        return i;
    }

    private static int stringEnd(Buffer json, int start) {
        int i = start;

        while (true) {
            byte c = json.getByte(i);

            if (c == '"') {
                return i;
            }

            i++;
        }
    }

    private static boolean containsStringElementFast(
            Buffer json,
            int arrayStart,
            int arrayEnd,
            int targetStart,
            int targetEnd
    ) {
        int targetLen = targetEnd - targetStart;

        for (int i = arrayStart + 1; i < arrayEnd - targetLen; i++) {
            if (json.getByte(i) == '"'
                    && json.getByte(i + targetLen + 1) == '"'
                    && regionEquals(json, i + 1, targetStart, targetLen)) {
                return true;
            }
        }

        return false;
    }

    private static boolean regionEquals(Buffer json, int aStart, int bStart, int len) {
        for (int i = 0; i < len; i++) {
            if (json.getByte(aStart + i) != json.getByte(bStart + i)) {
                return false;
            }
        }

        return true;
    }
}