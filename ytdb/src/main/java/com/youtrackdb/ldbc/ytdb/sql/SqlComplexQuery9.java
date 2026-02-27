package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery9;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery9Result;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * IC9: Recent messages by friends or friends of friends.
 * Same structure as IC2 but traverses 2 hops instead of 1.
 */
public class SqlComplexQuery9
    implements OperationHandler<LdbcQuery9, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery9 operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var results = state.computeInTx(graph -> {
        var rows = query(graph, LdbcQuerySql.IC9,
            "personId", operation.getPersonIdQ9(),
            "maxDate", operation.getMaxDate(),
            "limit", operation.getLimit());
        return rows.stream().map(row -> new LdbcQuery9Result(
            toLong(row.get("personId")),
            toStr(row.get("firstName")),
            toStr(row.get("lastName")),
            toLong(row.get("messageId")),
            toStr(row.get("messageContent")),
            toDateMillis(row.get("messageCreationDate"))
        )).toList();
      });
      resultReporter.report(results.size(), results, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 9", e);
    }
  }
}
