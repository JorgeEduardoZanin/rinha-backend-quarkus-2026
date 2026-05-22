package jorge.rinha.backend.enums;

public enum MerchantCategoryCode {

    MCC_5411(5411, 0.15f),
    MCC_5812(5812, 0.30f),
    MCC_5912(5912, 0.20f),
    MCC_5944(5944, 0.45f),
    MCC_7801(7801, 0.80f),
    MCC_7802(7802, 0.75f),
    MCC_7995(7995, 0.85f),
    MCC_4511(4511, 0.35f),
    MCC_5311(5311, 0.25f),
    MCC_5999(5999, 0.50f);

    private final int code;
    private final float value;

    MerchantCategoryCode(int code, float value) {
        this.code = code;
        this.value = value;
    }

    public int getCode() {
        return code;
    }

    public float getValue() {
        return value;
    }

    // Performance máxima: resolvido diretamente na Stack, em tempo de CPU puro, sem alocação
    public static float getValueByCode(String code) {
        return switch (code) {
            case "5411" -> 0.15f;
            case "5812" -> 0.30f;
            case "5912" -> 0.20f;
            case "5944" -> 0.45f;
            case "7801" -> 0.80f;
            case "7802" -> 0.75f;
            case "7995" -> 0.85f;
            case "4511" -> 0.35f;
            case "5311" -> 0.25f;
            case "5999" -> 0.50f;
            default   -> 0.5f;
        };
    }
}