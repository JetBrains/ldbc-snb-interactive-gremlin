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
      LdbcUpdate4AddForum operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      state.executeInTx(graph -> {
        exec(graph, LdbcQuerySql.U4,
            "forumId", operation.getForumId(),
            "title", operation.getForumTitle(),
            "creationDate", operation.getCreationDate(),
            "moderatorId", operation.getModeratorPersonId());

        for (Long tagId : operation.getTagIds()) {
          exec(graph, LdbcQuerySql.U4_HAS_TAG, "forumId", operation.getForumId(), "tagId", tagId);
        }
      });
      resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Update 4", e);
    }
  }
}
