package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery5;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery5Result;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * IC5: New groups.
 * Find Forums that friends/fof joined after minDate, count their Posts.
 */
public class SqlComplexQuery5
    implements OperationHandler<LdbcQuery5, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery5 operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var results = state.computeInTx(graph -> {
        var rows = query(graph, LdbcQuerySql.IC5,
            "personId", operation.getPersonIdQ5(),
            "minDate", operation.getMinDate(),
            "limit", operation.getLimit());
        return rows.stream().map(row -> new LdbcQuery5Result(
            toStr(row.get("forumTitle")),
            toInt(row.get("postCount"))
        )).toList();
      });
      resultReporter.report(results.size(), results, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 5", e);
    }
  }
}
