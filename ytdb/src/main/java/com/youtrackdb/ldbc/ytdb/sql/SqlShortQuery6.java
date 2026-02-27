package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery6MessageForum;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery6MessageForumResult;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

public class SqlShortQuery6
    implements OperationHandler<LdbcShortQuery6MessageForum, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcShortQuery6MessageForum operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var result = state.computeInTx(graph -> {
        // Step 1: Find the original Post ID (handles both Post and Comment messages).
        // WHILE-recursive match results can project properties but cannot serve as
        // starting vertices for subsequent edge traversal in the same MATCH chain,
        // so we split into two queries.
        var postRow = querySingle(graph, LdbcQuerySql.IS6_POST,
            "messageId", operation.getMessageForumId());
        if (postRow == null) {
          throw new DbException(
              "No message found for ID: " + operation.getMessageForumId());
        }
        long postId = toLong(postRow.get("postId"));

        // Step 2: Look up Forum and Moderator from the Post.
        var row = querySingle(graph, LdbcQuerySql.IS6_FORUM, "postId", postId);
        if (row == null) {
          throw new DbException(
              "No forum found for Post ID: " + postId
                  + " (message ID: " + operation.getMessageForumId() + ")");
        }
        return new LdbcShortQuery6MessageForumResult(
            toLong(row.get("forumId")),
            toStr(row.get("forumTitle")),
            toLong(row.get("moderatorId")),
            toStr(row.get("moderatorFirstName")),
            toStr(row.get("moderatorLastName"))
        );
      });
      resultReporter.report(0, result, operation);
    } catch (Exception e) {
      throw new DbException("Error executing Baseline Short Query 6", e);
    }
  }
}
