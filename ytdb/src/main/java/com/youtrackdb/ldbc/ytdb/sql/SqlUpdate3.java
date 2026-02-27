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
      LdbcUpdate3AddCommentLike op,
      TinkerPopConnectionState state,
      ResultReporter rr) throws DbException {
    try {
      state.executeInTx(g -> exec(g, LdbcQuerySql.U3,
          "personId", op.getPersonId(),
          "commentId", op.getCommentId(),
          "creationDate", op.getCreationDate()));
      rr.report(0, LdbcNoResult.INSTANCE, op);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing SQL Update 3", e);
    }
  }
}
