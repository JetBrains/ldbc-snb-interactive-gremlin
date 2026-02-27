package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery7;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery7Result;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * IC7: Recent likers.
 * Find the most recent Likes on start Person's Messages. Computes
 * minutesLatency = (likeDate - messageCreationDate) / 60000.
 * The SQL returns latestLikeDate, messageCreationDate, and isNew flag.
 */
public class SqlComplexQuery7
    implements OperationHandler<LdbcQuery7, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery7 operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var results = state.computeInTx(graph -> {
        var rows = query(graph, LdbcQuerySql.IC7,
            "personId", operation.getPersonIdQ7(),
            "limit", operation.getLimit());
        return rows.stream().map(row -> {
          long likeDate = toDateMillis(row.get("likeCreationDate"));
          long msgDate = toDateMillis(row.get("messageCreationDate"));
          int minutesLatency = (int) ((likeDate - msgDate) / 60000);
          return new LdbcQuery7Result(
              toLong(row.get("personId")),
              toStr(row.get("firstName")),
              toStr(row.get("lastName")),
              likeDate,
              toLong(row.get("messageId")),
              toStr(row.get("messageContent")),
              minutesLatency,
              toBool(row.get("isNew"))
          );
        }).toList();
      });
      resultReporter.report(results.size(), results, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 7", e);
    }
  }

}
