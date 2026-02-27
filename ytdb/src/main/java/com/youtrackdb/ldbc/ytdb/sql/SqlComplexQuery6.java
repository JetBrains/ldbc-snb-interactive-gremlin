package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery6;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery6Result;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * IC6: Tag co-occurrence.
 * Find other Tags that co-occur with a given Tag on Posts by friends/fof.
 */
public class SqlComplexQuery6
    implements OperationHandler<LdbcQuery6, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery6 operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var results = state.computeInTx(graph -> {
        var rows = query(graph, LdbcQuerySql.IC6,
            "personId", operation.getPersonIdQ6(),
            "tagName", operation.getTagName(),
            "limit", operation.getLimit());
        return rows.stream().map(row -> new LdbcQuery6Result(
            toStr(row.get("tagName")),
            toInt(row.get("postCount"))
        )).toList();
      });
      resultReporter.report(results.size(), results, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 6", e);
    }
  }
}
