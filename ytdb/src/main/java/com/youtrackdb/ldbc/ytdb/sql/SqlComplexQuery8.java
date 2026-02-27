package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery8;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery8Result;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * IC8: Recent replies.
 * Find the most recent Comments that are replies to Messages of the start Person.
 */
public class SqlComplexQuery8
    implements OperationHandler<LdbcQuery8, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery8 operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var results = state.computeInTx(graph -> {
        var rows = query(graph, LdbcQuerySql.IC8,
            "personId", operation.getPersonIdQ8(),
            "limit", operation.getLimit());
        return rows.stream().map(row -> new LdbcQuery8Result(
            toLong(row.get("personId")),
            toStr(row.get("firstName")),
            toStr(row.get("lastName")),
            toDateMillis(row.get("commentCreationDate")),
            toLong(row.get("commentId")),
            toStr(row.get("commentContent"))
        )).toList();
      });
      resultReporter.report(results.size(), results, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 8", e);
    }
  }
}
