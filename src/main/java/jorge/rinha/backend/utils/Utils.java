package jorge.rinha.backend.utils;

public class Utils {

    public static int getDayOfWeek(int year, int month, int day) {
        if (month < 3) {
            month += 12;
            year--;
        }

        int k = year % 100;
        int j = year / 100;

        int h = (day + (13 * (month + 1)) / 5 + k + (k / 4) + (j / 4) - 2 * j) % 7;

        return ((h + 5) % 7 + 7) % 7;
    }

    public static long toEpochSeconds(String iso) {
        int year   = ((iso.charAt(0) - '0') * 1000)
                + ((iso.charAt(1) - '0') * 100)
                + ((iso.charAt(2) - '0') * 10)
                +  (iso.charAt(3) - '0');

        int month  = ((iso.charAt(5) - '0') * 10)
                +  (iso.charAt(6) - '0');

        int day    = ((iso.charAt(8) - '0') * 10)
                +  (iso.charAt(9) - '0');

        int hour   = ((iso.charAt(11) - '0') * 10)
                +  (iso.charAt(12) - '0');

        int minute = ((iso.charAt(14) - '0') * 10)
                +  (iso.charAt(15) - '0');

        int second = ((iso.charAt(17) - '0') * 10)
                +  (iso.charAt(18) - '0');

        return toEpochSeconds(year, month, day, hour, minute, second);
    }

    public static long toEpochSeconds(
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

    public static float clamp(float v) {
        if (v != v) return 0.0f;
        if (v > 1.0f) return 1.0f;
        if (v < 0.0f) return 0.0f;
        return v;
    }
}