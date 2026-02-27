package com.youtrackdb.ldbc.ytdb;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper;

import java.util.List;
import java.util.Map;

final class LdbcQuerySqlProfilerSupport {

  static final long PERSON_ID = 933L;
  static final long MESSAGE_ID = 2061584476422L;

  private static final int WARMUP_ITERATIONS = 3;
  private static final int TIMED_ITERATIONS = 5;

  private LdbcQuerySqlProfilerSupport() {
  }

  static void profileSql(
      YTDBGraphTraversalSource g,
      String queryName,
      String sql,
      Map<String, Object> params) throws Exception {
    Object[] keyValues = flattenParams(params);
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      g.computeInTx(gts -> SqlResultHelper.query(gts, sql, keyValues));
    }

    long totalNanos = 0;
    List<Map<String, Object>> lastResults = null;
    for (int i = 0; i < TIMED_ITERATIONS; i++) {
      long start = System.nanoTime();
      lastResults = g.computeInTx(gts -> SqlResultHelper.query(gts, sql, keyValues));
      totalNanos += System.nanoTime() - start;
    }

    double avgMs = (totalNanos / (double) TIMED_ITERATIONS) / 1e6;
    int resultCount = lastResults != null ? lastResults.size() : 0;
    System.out.printf("  %s SQL: %.2f ms avg (%d results)%n",
        queryName, avgMs, resultCount);
  }

  private static Object[] flattenParams(Map<String, Object> params) {
    Object[] result = new Object[params.size() * 2];
    int i = 0;
    for (var entry : params.entrySet()) {
      result[i++] = entry.getKey();
      result[i++] = entry.getValue();
    }
    return result;
  }
}
