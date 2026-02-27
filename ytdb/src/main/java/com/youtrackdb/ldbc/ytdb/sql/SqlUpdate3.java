package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate3AddCommentLike;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * U3: Add Comment Like.
 */
public class SqlUpdate3
    implements OperationHandler<LdbcUpdate3AddCommentLike, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcUpdate3AddCommentLike operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      state.executeInTx(graph -> exec(graph, LdbcQuerySql.U3,
          "personId", operation.getPersonId(),
          "commentId", operation.getCommentId(),
          "creationDate", operation.getCreationDate()));
      resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Update 3", e);
    }
  }
}
