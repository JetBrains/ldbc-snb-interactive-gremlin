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
      LdbcUpdate6AddPost operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      state.executeInTx(graph -> {
        exec(graph, LdbcQuerySql.U6,
            "postId", operation.getPostId(),
            "creationDate", operation.getCreationDate(),
            "locationIP", operation.getLocationIp(),
            "browserUsed", operation.getBrowserUsed(),
            "language", operation.getLanguage(),
            "content", operation.getContent(),
            "imageFile", operation.getImageFile(),
            "length", operation.getLength(),
            "authorPersonId", operation.getAuthorPersonId(),
            "forumId", operation.getForumId(),
            "countryId", operation.getCountryId());

        for (Long tagId : operation.getTagIds()) {
          exec(graph, LdbcQuerySql.U6_HAS_TAG, "postId", operation.getPostId(), "tagId", tagId);
        }
      });
      resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Update 6", e);
    }
  }
}
