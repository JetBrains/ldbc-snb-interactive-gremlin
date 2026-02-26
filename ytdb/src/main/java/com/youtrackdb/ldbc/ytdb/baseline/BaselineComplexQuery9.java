package com.youtrackdb.ldbc.ytdb.baseline;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery9;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery9Result;

import static com.youtrackdb.ldbc.ytdb.baseline.SqlResultHelper.*;

/**
 * IC9: Recent messages by friends or friends of friends.
 * Same structure as IC2 but traverses 2 hops instead of 1.
 */
public class BaselineComplexQuery9
    implements OperationHandler<LdbcQuery9, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery9 op,
      TinkerPopConnectionState state,
      ResultReporter rr) throws DbException {
    try {
      var results = state.computeInTx(g -> {
        var rows = query(g, LdbcQuerySql.IC9,
            "personId", op.getPersonIdQ9(),
            "maxDate", op.getMaxDate(),
            "limit", op.getLimit());
        return rows.stream().map(row -> new LdbcQuery9Result(
            toLong(row.get("personId")),
            toStr(row.get("firstName")),
            toStr(row.get("lastName")),
            toLong(row.get("messageId")),
            toStr(row.get("messageContent")),
            toDateMillis(row.get("messageCreationDate"))
        )).toList();
      });
      rr.report(results.size(), results, op);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 9", e);
    }
  }
}
