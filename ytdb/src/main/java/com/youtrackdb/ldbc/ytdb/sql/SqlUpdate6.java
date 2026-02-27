package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate6AddPost;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * U6: Add Post.
 * Creates a Post vertex with HAS_CREATOR, CONTAINER_OF, IS_LOCATED_IN edges,
 * plus variable-length HAS_TAG edges.
 */
public class SqlUpdate6
    implements OperationHandler<LdbcUpdate6AddPost, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcUpdate6AddPost op,
      TinkerPopConnectionState state,
      ResultReporter rr) throws DbException {
    try {
      state.executeInTx(g -> {
        exec(g, LdbcQuerySql.U6,
            "postId", op.getPostId(),
            "creationDate", op.getCreationDate(),
            "locationIP", op.getLocationIp(),
            "browserUsed", op.getBrowserUsed(),
            "language", op.getLanguage(),
            "content", op.getContent(),
            "imageFile", op.getImageFile(),
            "length", op.getLength(),
            "authorPersonId", op.getAuthorPersonId(),
            "forumId", op.getForumId(),
            "countryId", op.getCountryId());

        for (Long tagId : op.getTagIds()) {
          exec(g, LdbcQuerySql.U6_HAS_TAG, "postId", op.getPostId(), "tagId", tagId);
        }
      });
      rr.report(0, LdbcNoResult.INSTANCE, op);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing SQL Update 6", e);
    }
  }
}
