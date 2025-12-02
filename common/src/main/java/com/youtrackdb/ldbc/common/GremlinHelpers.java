package com.youtrackdb.ldbc.common;

import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;

public final class GremlinHelpers {

    private GremlinHelpers() {}

    public static Date plusDays(Date date, int days) {
        return new Date(date.toInstant().plus(days, ChronoUnit.DAYS).toEpochMilli());
    }

    public static long dateToMillis(Object dateObj) {
        if (dateObj == null) {
            return 0L;
        }
        if (dateObj instanceof Date date) {
            return date.getTime();
        }
        if (dateObj instanceof Long l) {
            return l;
        }
        if (dateObj instanceof Number n) {
            return n.longValue();
        }
        throw new IllegalArgumentException("Cannot convert to date millis: " + dateObj.getClass());
    }

    public static long getLong(Map<String, Object> record, String key) {
        Object value = record.get(key);
        if (value == null) {
            return 0L;
        }
        if (value instanceof Number n) {
            return n.longValue();
        }

        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cannot convert to long: " + value.getClass() + " with value: " + value, e);
        }
    }

    public static String getString(Map<String, Object> record, String key) {
        Object value = record.get(key);
        return value != null ? value.toString() : "";
    }

    public static long getDateAsMillis(Map<String, Object> record, String key) {
        return dateToMillis(record.get(key));
    }

    public static int getInt(Map<String, Object> record, String key) {
        Object value = record.get(key);
        if (value == null) {
            return 0;
        }
        if (value instanceof Number n) {
            return n.intValue();
        }
        throw new IllegalArgumentException("Cannot convert to int: " + value.getClass());
    }

    public static boolean getBoolean(Map<String, Object> record, String key) {
        Object value = record.get(key);
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean b) {
            return b;
        }
        throw new IllegalArgumentException("Cannot convert to boolean: " + value.getClass());
    }
}
