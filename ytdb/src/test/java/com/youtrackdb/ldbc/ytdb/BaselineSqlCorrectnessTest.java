package com.youtrackdb.ldbc.ytdb;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import com.youtrackdb.ldbc.ytdb.sql.LdbcQuerySql;
import com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Correctness tests for SQL queries.
 * <p>
 * Unlike the profiling tests, these exercise the full SQL + Java conversion
 * pipeline (toInt, toLong, toOrganizations, etc.) and assert that queries
 * return expected results without parsing errors.
 */
@Tag("profiling")
@ExtendWith(LdbcSnbDatabaseExtension.class)
class BaselineSqlCorrectnessTest {

  // ---- IS6: Message Forum (two-step: find Post, then Forum) ----

  @Test
  void is6_returnsResultForComment(YTDBGraphTraversalSource g) {
    Long commentId = g.computeInTx(gts -> {
      var rows = SqlResultHelper.query(gts, "SELECT id FROM Comment LIMIT 1");
      return rows.isEmpty() ? null : SqlResultHelper.toLong(rows.getFirst().get("id"));
    });
    assertNotNull(commentId, "Test data should contain at least one Comment");

    var result = g.computeInTx(gts -> {
      var postRow = SqlResultHelper.querySingle(gts, LdbcQuerySql.IS6_POST,
          "messageId", commentId);
      assertNotNull(postRow, "IS6_POST should find the original Post for Comment " + commentId);
      long postId = SqlResultHelper.toLong(postRow.get("postId"));
      return SqlResultHelper.querySingle(gts, LdbcQuerySql.IS6_FORUM, "postId", postId);
    });

    assertNotNull(result, "IS6_FORUM should return a result for Comment ID " + commentId);
    assertNotNull(result.get("forumId"), "forumId should be present");
    assertNotNull(result.get("moderatorId"), "moderatorId should be present");
  }

  @Test
  void is6_returnsResultForPost(YTDBGraphTraversalSource g) {
    Long postId = g.computeInTx(gts -> {
      var rows = SqlResultHelper.query(gts, "SELECT id FROM Post LIMIT 1");
      return rows.isEmpty() ? null : SqlResultHelper.toLong(rows.getFirst().get("id"));
    });
    assertNotNull(postId, "Test data should contain at least one Post");

    var result = g.computeInTx(gts -> {
      var postRow = SqlResultHelper.querySingle(gts, LdbcQuerySql.IS6_POST,
          "messageId", postId);
      assertNotNull(postRow, "IS6_POST should match the Post itself at depth 0 for Post " + postId);
      long resolvedPostId = SqlResultHelper.toLong(postRow.get("postId"));
      assertEquals(postId, resolvedPostId, "Post ID should resolve to itself");
      return SqlResultHelper.querySingle(gts, LdbcQuerySql.IS6_FORUM, "postId", resolvedPostId);
    });

    assertNotNull(result, "IS6_FORUM should return a result for Post ID " + postId);
    assertNotNull(result.get("forumId"), "forumId should be present");
    assertNotNull(result.get("moderatorId"), "moderatorId should be present");
  }

  // ---- IC1: Transitive friends with certain name ----

  @Test
  void ic1_parsesOrganizationsWithoutError(YTDBGraphTraversalSource g) {
    // Use first row from substitution params: personId=30786325579101, firstName=Ian
    var rows = g.computeInTx(gts -> SqlResultHelper.query(gts, LdbcQuerySql.IC1,
        "personId", 30786325579101L,
        "firstName", "Ian",
        "limit", 20));

    assertNotNull(rows);
    for (var row : rows) {
      // These are the fields that caused NumberFormatException when
      // the SQL returned "[2005]" instead of 2005
      Object universities = row.get("universities");
      Object companies = row.get("companies");

      if (universities instanceof List<?> uniList) {
        for (Object item : uniList) {
          if (item instanceof Map<?, ?> m) {
            assertDoesNotThrow(() -> SqlResultHelper.toInt(m.get("classYear")),
                "classYear should be parseable as int, got: " + m.get("classYear"));
            assertNotNull(m.get("uniName"), "uniName should be present");
            assertNotNull(m.get("uniCityName"), "uniCityName should be present");
          }
        }
      }

      if (companies instanceof List<?> compList) {
        for (Object item : compList) {
          if (item instanceof Map<?, ?> m) {
            assertDoesNotThrow(() -> SqlResultHelper.toInt(m.get("workFromYear")),
                "workFromYear should be parseable as int, got: " + m.get("workFromYear"));
            assertNotNull(m.get("compName"), "compName should be present");
            assertNotNull(m.get("compCountryName"), "compCountryName should be present");
          }
        }
      }
    }
  }

  @Test
  void ic1_fieldsParseWithoutError(YTDBGraphTraversalSource g) {
    var rows = g.computeInTx(gts -> SqlResultHelper.query(gts, LdbcQuerySql.IC1,
        "personId", 30786325579101L,
        "firstName", "Ian",
        "limit", 20));

    assertNotNull(rows);
    for (var row : rows) {
      assertDoesNotThrow(() -> SqlResultHelper.toLong(row.get("personId")));
      assertDoesNotThrow(() -> SqlResultHelper.toStr(row.get("lastName")));
      assertDoesNotThrow(() -> SqlResultHelper.toInt(row.get("distance")));
      assertDoesNotThrow(() -> SqlResultHelper.toDateMillis(row.get("birthday")));
      assertDoesNotThrow(() -> SqlResultHelper.toDateMillis(row.get("creationDate")));
    }
  }
}
