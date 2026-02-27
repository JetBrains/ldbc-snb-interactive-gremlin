package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.GremlinHelpers;
import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery3;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery3Result;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * IC3: Friends and friends of friends that have been to given countries.
 * LDBC gives startDate + durationDays; the SQL needs startDate + endDate.
 */
public class SqlComplexQuery3
    implements OperationHandler<LdbcQuery3, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery3 operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var endDate = GremlinHelpers.plusDays(operation.getStartDate(), operation.getDurationDays());
      var results = state.computeInTx(graph -> {
        var rows = query(graph, LdbcQuerySql.IC3,
            "personId", operation.getPersonIdQ3(),
            "countryX", operation.getCountryXName(),
            "countryY", operation.getCountryYName(),
            "startDate", operation.getStartDate(),
            "endDate", endDate,
            "limit", operation.getLimit());
        return rows.stream().map(row -> new LdbcQuery3Result(
            toLong(row.get("personId")),
            toStr(row.get("firstName")),
            toStr(row.get("lastName")),
            toLong(row.get("xCount")),
            toLong(row.get("yCount")),
            toLong(row.get("totalCount"))
        )).toList();
      });
      resultReporter.report(results.size(), results, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 3", e);
    }
  }
}
