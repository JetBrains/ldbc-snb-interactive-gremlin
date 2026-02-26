package com.youtrackdb.ldbc.ytdb.baseline;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery10;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery10Result;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static com.youtrackdb.ldbc.ytdb.baseline.SqlResultHelper.*;

/**
 * IC10: Friend recommendation.
 * LDBC gives month (1-12). The LDBC spec says: "born on or after the 21st of
 * a given month and before the 22nd of the following month."
 * The SQL uses birthday date range (startDate, endDate). We compute those
 * from the month parameter using a fixed reference year (1980) since only
 * the month/day portion matters for birthday comparisons in this dataset.
 */
public class BaselineComplexQuery10
    implements OperationHandler<LdbcQuery10, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery10 op,
      TinkerPopConnectionState state,
      ResultReporter rr) throws DbException {
    try {
      int month = op.getMonth();
      int nextMonth = (month % 12) + 1;

      var startDate = buildDate(month, 21);
      var endDate = buildDate(nextMonth, 22);

      var results = state.computeInTx(g -> {
        var rows = query(g, LdbcQuerySql.IC10,
            "personId", op.getPersonIdQ10(),
            "startDate", startDate,
            "endDate", endDate,
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

  private static Date buildDate(int month, int day) {
    var cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    cal.set(1980, month - 1, day, 0, 0, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }
}
