package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery1PersonProfile;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery1PersonProfileResult;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

public class SqlShortQuery1
    implements OperationHandler<LdbcShortQuery1PersonProfile, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcShortQuery1PersonProfile operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var result = state.computeInTx(graph -> {
        var row = querySingle(graph, LdbcQuerySql.IS1,
            "personId", operation.getPersonIdSQ1());
        if (row == null) {
          throw new DbException("No person found with ID: " + operation.getPersonIdSQ1());
        }
        return new LdbcShortQuery1PersonProfileResult(
            toStr(row.get("firstName")),
            toStr(row.get("lastName")),
            toDateMillis(row.get("birthday")),
            toStr(row.get("locationIP")),
            toStr(row.get("browserUsed")),
            toLong(row.get("cityId")),
            toStr(row.get("gender")),
            toDateMillis(row.get("creationDate"))
        );
      });
      resultReporter.report(0, result, operation);
    } catch (Exception e) {
      throw new DbException("Error executing Baseline Short Query 1", e);
    }
  }
}
