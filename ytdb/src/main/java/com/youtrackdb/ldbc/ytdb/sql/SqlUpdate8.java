package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate8AddFriendship;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * U8: Add Friendship.
 * Creates bidirectional KNOWS edges between two persons.
 */
public class SqlUpdate8
    implements OperationHandler<LdbcUpdate8AddFriendship, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcUpdate8AddFriendship operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      state.executeInTx(graph -> exec(graph, LdbcQuerySql.U8,
          "person1Id", operation.getPerson1Id(),
          "person2Id", operation.getPerson2Id(),
          "creationDate", operation.getCreationDate()));
      resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Update 8", e);
    }
  }
}
