package com.youtrackdb.ldbc.ytdb.baseline;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Loads all LDBC SNB Interactive query SQL from classpath resources.
 * Short queries (IS1-IS7) and complex queries (IC1-IC13).
 */
public final class LdbcQuerySql {

  // Interactive Short queries
  public static final String IS1 = loadResource("ldbc-queries/IS1.sql");
  public static final String IS2 = loadResource("ldbc-queries/IS2.sql");
  public static final String IS3 = loadResource("ldbc-queries/IS3.sql");
  public static final String IS4 = loadResource("ldbc-queries/IS4.sql");
  public static final String IS5 = loadResource("ldbc-queries/IS5.sql");
  public static final String IS6 = loadResource("ldbc-queries/IS6.sql");
  public static final String IS7 = loadResource("ldbc-queries/IS7.sql");

  // Interactive Complex queries
  public static final String IC1 = loadResource("ldbc-queries/IC1.sql");
  public static final String IC2 = loadResource("ldbc-queries/IC2.sql");
  public static final String IC3 = loadResource("ldbc-queries/IC3.sql");
  public static final String IC4 = loadResource("ldbc-queries/IC4.sql");
  public static final String IC5 = loadResource("ldbc-queries/IC5.sql");
  public static final String IC6 = loadResource("ldbc-queries/IC6.sql");
  public static final String IC7 = loadResource("ldbc-queries/IC7.sql");
  public static final String IC8 = loadResource("ldbc-queries/IC8.sql");
  public static final String IC9 = loadResource("ldbc-queries/IC9.sql");
  public static final String IC10 = loadResource("ldbc-queries/IC10.sql");
  public static final String IC11 = loadResource("ldbc-queries/IC11.sql");
  public static final String IC12 = loadResource("ldbc-queries/IC12.sql");
  public static final String IC13 = loadResource("ldbc-queries/IC13.sql");

  private LdbcQuerySql() {}

  private static String loadResource(String path) {
    try (var is = LdbcQuerySql.class.getClassLoader().getResourceAsStream(path)) {
      if (is == null) {
        throw new IllegalStateException("SQL resource not found: " + path);
      }
      return new String(is.readAllBytes(), StandardCharsets.UTF_8).trim();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
