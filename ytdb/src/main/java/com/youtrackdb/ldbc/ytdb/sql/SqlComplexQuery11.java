package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery11;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery11Result;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * IC11: Job referral.
 * Find friends/fof who started working in a Company in a given Country
 * before a given year.
 */
public class SqlComplexQuery11
    implements OperationHandler<LdbcQuery11, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery11 operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var results = state.computeInTx(graph -> {
        var rows = query(graph, LdbcQuerySql.IC11,
            "personId", operation.getPersonIdQ11(),
            "workFromYear", operation.getWorkFromYear(),
            "countryName", operation.getCountryName(),
            "limit", operation.getLimit());
        return rows.stream().map(row -> new LdbcQuery11Result(
            toLong(row.get("personId")),
            toStr(row.get("firstName")),
            toStr(row.get("lastName")),
            toStr(row.get("organizationName")),
            toInt(row.get("organizationWorkFromYear"))
        )).toList();
      });
      resultReporter.report(results.size(), results, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 11", e);
    }
  }
}
