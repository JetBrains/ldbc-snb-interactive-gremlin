package com.youtrackdb.ldbc.ytdb.baseline;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Shared helpers for executing SQL via {@code sqlCommand()} and converting
 * result values from {@code Map<String, Object>} rows.
 */
public final class SqlResultHelper {

  private SqlResultHelper() {}

  /**
   * Casts a generic {@code GraphTraversalSource} (from {@code computeInTx}) to
   * {@code YTDBGraphTraversalSource} to access {@code sqlCommand()}.
   */
  public static YTDBGraphTraversalSource ytdb(GraphTraversalSource g) {
    return (YTDBGraphTraversalSource) g;
  }

  /**
   * Executes SQL via {@code sqlCommand()} and returns all rows as
   * {@code List<Map<String, Object>>}.
   */
  @SuppressWarnings("unchecked")
  public static List<Map<String, Object>> query(
      GraphTraversalSource g, String sql, Object... keyValues) {
    return ytdb(g).sqlCommand(sql, keyValues).toList().stream()
        .map(obj -> (Map<String, Object>) obj)
        .toList();
  }

  /**
   * Executes SQL via {@code sqlCommand()} and returns the first row, or null.
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> querySingle(
      GraphTraversalSource g, String sql, Object... keyValues) {
    var results = ytdb(g).sqlCommand(sql, keyValues).toList();
    if (results.isEmpty()) return null;
    return (Map<String, Object>) results.getFirst();
  }

  public static long toLong(Object value) {
    if (value == null) return 0L;
    if (value instanceof Number n) return n.longValue();
    return Long.parseLong(value.toString());
  }

  public static String toStr(Object value) {
    return value != null ? value.toString() : "";
  }

  public static long toDateMillis(Object value) {
    if (value == null) return 0L;
    if (value instanceof Date d) return d.getTime();
    if (value instanceof Number n) return n.longValue();
    throw new IllegalArgumentException("Cannot convert to date millis: " + value.getClass());
  }

  public static boolean toBool(Object value) {
    if (value == null) return false;
    if (value instanceof Boolean b) return b;
    return false;
  }

  public static int toInt(Object value) {
    if (value == null) return 0;
    if (value instanceof Number n) return n.intValue();
    return Integer.parseInt(value.toString());
  }

  @SuppressWarnings("unchecked")
  public static List<String> toStringList(Object value) {
    if (value == null) return Collections.emptyList();
    if (value instanceof Collection<?> c) {
      return c.stream().map(Object::toString).toList();
    }
    return Collections.emptyList();
  }
}
