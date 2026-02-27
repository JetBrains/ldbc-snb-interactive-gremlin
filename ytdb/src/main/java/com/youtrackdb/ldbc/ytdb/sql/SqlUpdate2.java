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
      LdbcUpdate2AddPostLike op,
      TinkerPopConnectionState state,
      ResultReporter rr) throws DbException {
    try {
      state.executeInTx(g -> exec(g, LdbcQuerySql.U2,
          "personId", op.getPersonId(),
          "postId", op.getPostId(),
          "creationDate", op.getCreationDate()));
      rr.report(0, LdbcNoResult.INSTANCE, op);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing SQL Update 2", e);
    }
  }
}
