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
      LdbcQuery3 op,
      TinkerPopConnectionState state,
      ResultReporter rr) throws DbException {
    try {
      var endDate = GremlinHelpers.plusDays(op.getStartDate(), op.getDurationDays());
      var results = state.computeInTx(g -> {
        var rows = query(g, LdbcQuerySql.IC3,
            "personId", op.getPersonIdQ3(),
            "countryX", op.getCountryXName(),
            "countryY", op.getCountryYName(),
            "startDate", op.getStartDate(),
            "endDate", endDate,
            "limit", op.getLimit());
        return rows.stream().map(row -> new LdbcQuery3Result(
            toLong(row.get("personId")),
            toStr(row.get("firstName")),
            toStr(row.get("lastName")),
            toLong(row.get("xCount")),
            toLong(row.get("yCount")),
            toLong(row.get("totalCount"))
        )).toList();
      });
      rr.report(results.size(), results, op);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 3", e);
    }
  }
}
