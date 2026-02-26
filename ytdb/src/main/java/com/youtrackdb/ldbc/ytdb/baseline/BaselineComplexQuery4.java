package com.youtrackdb.ldbc.ytdb.baseline;

import com.youtrackdb.ldbc.common.GremlinHelpers;
import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery4;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery4Result;

import static com.youtrackdb.ldbc.ytdb.baseline.SqlResultHelper.*;

/**
 * IC4: New topics.
 * LDBC gives startDate + durationDays; the SQL needs startDate + endDate.
 */
public class BaselineComplexQuery4
    implements OperationHandler<LdbcQuery4, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery4 op,
      TinkerPopConnectionState state,
      ResultReporter rr) throws DbException {
    try {
      var endDate = GremlinHelpers.plusDays(op.getStartDate(), op.getDurationDays());
      var results = state.computeInTx(g -> {
        var rows = query(g, LdbcQuerySql.IC4,
            "personId", op.getPersonIdQ4(),
            "startDate", op.getStartDate(),
            "endDate", endDate,
            "limit", op.getLimit());
        return rows.stream().map(row -> new LdbcQuery4Result(
            toStr(row.get("tagName")),
            toInt(row.get("postCount"))
        )).toList();
      });
      rr.report(results.size(), results, op);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 4", e);
    }
  }
}
