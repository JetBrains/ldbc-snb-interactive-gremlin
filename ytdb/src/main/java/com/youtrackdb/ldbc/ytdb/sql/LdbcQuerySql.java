package com.youtrackdb.ldbc.ytdb.sql;

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
  public static final String IS6_POST = loadResource("ldbc-queries/IS6_post.sql");
  public static final String IS6_FORUM = loadResource("ldbc-queries/IS6_forum.sql");
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

  // Update queries
  public static final String U1 = loadResource("ldbc-queries/U1.sql");
  public static final String U1_HAS_INTEREST = loadResource("ldbc-queries/U1_hasInterest.sql");
  public static final String U1_STUDY_AT = loadResource("ldbc-queries/U1_studyAt.sql");
  public static final String U1_WORK_AT = loadResource("ldbc-queries/U1_workAt.sql");
  public static final String U2 = loadResource("ldbc-queries/U2.sql");
  public static final String U3 = loadResource("ldbc-queries/U3.sql");
  public static final String U4 = loadResource("ldbc-queries/U4.sql");
  public static final String U4_HAS_TAG = loadResource("ldbc-queries/U4_hasTag.sql");
  public static final String U5 = loadResource("ldbc-queries/U5.sql");
  public static final String U6 = loadResource("ldbc-queries/U6.sql");
  public static final String U6_HAS_TAG = loadResource("ldbc-queries/U6_hasTag.sql");
  public static final String U7 = loadResource("ldbc-queries/U7.sql");
  public static final String U7_REPLY_OF_POST = loadResource("ldbc-queries/U7_replyOfPost.sql");
  public static final String U7_REPLY_OF_COMMENT = loadResource("ldbc-queries/U7_replyOfComment.sql");
  public static final String U7_HAS_TAG = loadResource("ldbc-queries/U7_hasTag.sql");
  public static final String U8 = loadResource("ldbc-queries/U8.sql");

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
