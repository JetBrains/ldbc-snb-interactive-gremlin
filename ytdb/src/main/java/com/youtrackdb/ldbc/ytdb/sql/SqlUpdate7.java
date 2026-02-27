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
      LdbcUpdate7AddComment op,
      TinkerPopConnectionState state,
      ResultReporter rr) throws DbException {
    try {
      state.executeInTx(g -> {
        exec(g, LdbcQuerySql.U7,
            "commentId", op.getCommentId(),
            "creationDate", op.getCreationDate(),
            "locationIP", op.getLocationIp(),
            "browserUsed", op.getBrowserUsed(),
            "content", op.getContent(),
            "length", op.getLength(),
            "authorPersonId", op.getAuthorPersonId(),
            "countryId", op.getCountryId());

        if (op.getReplyToPostId() != -1) {
          exec(g, LdbcQuerySql.U7_REPLY_OF_POST,
              "commentId", op.getCommentId(),
              "replyToPostId", op.getReplyToPostId());
        } else if (op.getReplyToCommentId() != -1) {
          exec(g, LdbcQuerySql.U7_REPLY_OF_COMMENT,
              "commentId", op.getCommentId(),
              "replyToCommentId", op.getReplyToCommentId());
        }

        for (Long tagId : op.getTagIds()) {
          exec(g, LdbcQuerySql.U7_HAS_TAG, "commentId", op.getCommentId(), "tagId", tagId);
        }
      });
      rr.report(0, LdbcNoResult.INSTANCE, op);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing SQL Update 7", e);
    }
  }
}
