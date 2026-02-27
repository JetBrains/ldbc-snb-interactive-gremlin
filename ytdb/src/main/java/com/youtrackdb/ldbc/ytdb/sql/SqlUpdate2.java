package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate2AddPostLike;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * U2: Add Post Like.
 */
public class SqlUpdate2
    implements OperationHandler<LdbcUpdate2AddPostLike, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcUpdate2AddPostLike operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      state.executeInTx(graph -> exec(graph, LdbcQuerySql.U2,
          "personId", operation.getPersonId(),
          "postId", operation.getPostId(),
          "creationDate", operation.getCreationDate()));
      resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Update 2", e);
    }
  }
}
