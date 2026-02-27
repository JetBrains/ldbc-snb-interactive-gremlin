package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate7AddComment;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * U7: Add Comment.
 * Creates a Comment vertex with HAS_CREATOR, IS_LOCATED_IN edges,
 * conditional REPLY_OF edge (to Post or Comment), plus variable-length HAS_TAG edges.
 */
public class SqlUpdate7
    implements OperationHandler<LdbcUpdate7AddComment, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcUpdate7AddComment operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      state.executeInTx(graph -> {
        exec(graph, LdbcQuerySql.U7,
            "commentId", operation.getCommentId(),
            "creationDate", operation.getCreationDate(),
            "locationIP", operation.getLocationIp(),
            "browserUsed", operation.getBrowserUsed(),
            "content", operation.getContent(),
            "length", operation.getLength(),
            "authorPersonId", operation.getAuthorPersonId(),
            "countryId", operation.getCountryId());

        if (operation.getReplyToPostId() != -1) {
          exec(graph, LdbcQuerySql.U7_REPLY_OF_POST,
              "commentId", operation.getCommentId(),
              "replyToPostId", operation.getReplyToPostId());
        } else if (operation.getReplyToCommentId() != -1) {
          exec(graph, LdbcQuerySql.U7_REPLY_OF_COMMENT,
              "commentId", operation.getCommentId(),
              "replyToCommentId", operation.getReplyToCommentId());
        }

        for (Long tagId : operation.getTagIds()) {
          exec(graph, LdbcQuerySql.U7_HAS_TAG, "commentId", operation.getCommentId(), "tagId", tagId);
        }
      });
      resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Update 7", e);
    }
  }
}
