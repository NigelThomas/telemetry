package com.sqlstream.utils.telemetry;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

import java.text.NumberFormat;
import java.util.Locale;

public class Utils {

    public static String formatLong(long l) {
        return String.format("%,d", l);
    }

    public static String formatDouble(double d) {
        return String.format("%,6g", d);
    }

    public static String humanReadableByteCountSI(long bytes, String suffix) {
        if (-1000 < bytes && bytes < 1000) {
            return bytes + " B";
        }
        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (bytes <= -999_950 || bytes >= 999_950) {
            bytes /= 1000;
            ci.next();
        }
        return String.format("%.1f %c%s", bytes / 1000.0, ci.current(), suffix);
    }

    public static String humanReadableByteCountBin(long bytes, String suffix) {
        long absB = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
        if (absB < 1024) {
            return bytes + " B";
        }
        long value = absB;
        CharacterIterator ci = new StringCharacterIterator("KMGTPE");
        for (int i = 40; i >= 0 && absB > 0xfffccccccccccccL >> i; i -= 10) {
            value >>= 10;
            ci.next();
        }
        value *= Long.signum(bytes);
        return String.format("%.1f %ci%s", value / 1024.0, ci.current(), suffix);
    }
}
