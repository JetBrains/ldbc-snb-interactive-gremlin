package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate4AddForum;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * U4: Add Forum.
 * Creates a Forum vertex with HAS_MODERATOR edge, plus variable-length HAS_TAG edges.
 */
public class SqlUpdate4
    implements OperationHandler<LdbcUpdate4AddForum, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcUpdate4AddForum op,
      TinkerPopConnectionState state,
      ResultReporter rr) throws DbException {
    try {
      state.executeInTx(g -> {
        exec(g, LdbcQuerySql.U4,
            "forumId", op.getForumId(),
            "title", op.getForumTitle(),
            "creationDate", op.getCreationDate(),
            "moderatorId", op.getModeratorPersonId());

        for (Long tagId : op.getTagIds()) {
          exec(g, LdbcQuerySql.U4_HAS_TAG, "forumId", op.getForumId(), "tagId", tagId);
        }
      });
      rr.report(0, LdbcNoResult.INSTANCE, op);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing SQL Update 4", e);
    }
  }
}
