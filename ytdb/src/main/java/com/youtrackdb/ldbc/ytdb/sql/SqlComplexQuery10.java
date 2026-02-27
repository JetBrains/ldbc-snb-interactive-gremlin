package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery10;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery10Result;


import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * IC10: Friend recommendation.
 * LDBC gives month (1-12). The LDBC spec says: "born on or after the 21st of
 * a given month and before the 22nd of the following month."
 * The SQL uses birthday date range (startDate, endDate). We compute those
 * from the month parameter using a fixed reference year (1980) since only
 * the month/day portion matters for birthday comparisons in this dataset.
 */
public class SqlComplexQuery10
    implements OperationHandler<LdbcQuery10, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery10 op,
      TinkerPopConnectionState state,
      ResultReporter rr) throws DbException {
    try {
      int month = op.getMonth();
      int nextMonth = (month % 12) + 1;
      String startMd = String.format("%02d%02d", month, 21);
      String endMd = String.format("%02d%02d", nextMonth, 22);
      boolean wrap = month == 12;

      var results = state.computeInTx(g -> {
        var rows = query(g, LdbcQuerySql.IC10,
            "personId", op.getPersonIdQ10(),
            "startMd", startMd,
            "endMd", endMd,
            "wrap", wrap,
            "limit", op.getLimit());
        return rows.stream().map(row -> new LdbcQuery10Result(
            toLong(row.get("personId")),
            toStr(row.get("firstName")),
            toStr(row.get("lastName")),
            toInt(row.get("commonInterestScore")),
            toStr(row.get("gender")),
            toStr(row.get("cityName"))
        )).toList();
      });
      rr.report(results.size(), results, op);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 10", e);
    }
  }

}
