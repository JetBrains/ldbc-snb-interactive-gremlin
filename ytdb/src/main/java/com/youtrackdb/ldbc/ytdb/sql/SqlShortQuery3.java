package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery3PersonFriends;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery3PersonFriendsResult;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

public class SqlShortQuery3
    implements OperationHandler<LdbcShortQuery3PersonFriends, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcShortQuery3PersonFriends operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var results = state.computeInTx(g -> {
        var rows = query(g, LdbcQuerySql.IS3,
            "personId", operation.getPersonIdSQ3());
        return rows.stream().map(row -> new LdbcShortQuery3PersonFriendsResult(
            toLong(row.get("personId")),
            toStr(row.get("firstName")),
            toStr(row.get("lastName")),
            toDateMillis(row.get("friendshipCreationDate"))
        )).toList();
      });
      resultReporter.report(results.size(), results, operation);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing Baseline Short Query 3", e);
    }
  }
}
