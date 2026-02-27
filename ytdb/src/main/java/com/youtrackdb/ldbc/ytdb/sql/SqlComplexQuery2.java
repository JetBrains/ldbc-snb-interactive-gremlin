package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery2;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery2Result;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * IC2: Recent messages by your friends.
 * Given a start Person, find the most recent Messages from all of that
 * Person's friends, created before a given maxDate.
 */
public class SqlComplexQuery2
    implements OperationHandler<LdbcQuery2, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery2 op,
      TinkerPopConnectionState state,
      ResultReporter rr) throws DbException {
    try {
      var results = state.computeInTx(g -> {
        var rows = query(g, LdbcQuerySql.IC2,
            "personId", op.getPersonIdQ2(),
            "maxDate", op.getMaxDate(),
            "limit", op.getLimit());
        return rows.stream().map(row -> new LdbcQuery2Result(
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
      throw new DbException("Error executing SQL Complex Query 2", e);
    }
  }
}
