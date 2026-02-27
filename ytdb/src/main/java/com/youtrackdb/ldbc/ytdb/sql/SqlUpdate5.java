package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate5AddForumMembership;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * U5: Add Forum Membership.
 */
public class SqlUpdate5
    implements OperationHandler<LdbcUpdate5AddForumMembership, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcUpdate5AddForumMembership operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      state.executeInTx(graph -> exec(graph, LdbcQuerySql.U5,
          "forumId", operation.getForumId(),
          "personId", operation.getPersonId(),
          "joinDate", operation.getJoinDate()));
      resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Update 5", e);
    }
  }
}
