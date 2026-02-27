package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.GremlinHelpers;
import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery4;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery4Result;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * IC4: New topics.
 * LDBC gives startDate + durationDays; the SQL needs startDate + endDate.
 */
public class SqlComplexQuery4
    implements OperationHandler<LdbcQuery4, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery4 operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var endDate = GremlinHelpers.plusDays(operation.getStartDate(), operation.getDurationDays());
      var results = state.computeInTx(graph -> {
        var rows = query(graph, LdbcQuerySql.IC4,
            "personId", operation.getPersonIdQ4(),
            "startDate", operation.getStartDate(),
            "endDate", endDate,
            "limit", operation.getLimit());
        return rows.stream().map(row -> new LdbcQuery4Result(
            toStr(row.get("tagName")),
            toInt(row.get("postCount"))
        )).toList();
      });
      resultReporter.report(results.size(), results, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 4", e);
    }
  }
}
