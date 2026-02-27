package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery7MessageReplies;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery7MessageRepliesResult;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

public class SqlShortQuery7
    implements OperationHandler<LdbcShortQuery7MessageReplies, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcShortQuery7MessageReplies operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var results = state.computeInTx(g -> {
        var rows = query(g, LdbcQuerySql.IS7,
            "messageId", operation.getMessageRepliesId());
        return rows.stream().map(row -> new LdbcShortQuery7MessageRepliesResult(
            toLong(row.get("commentId")),
            toStr(row.get("commentContent")),
            toDateMillis(row.get("commentCreationDate")),
            toLong(row.get("replyAuthorId")),
            toStr(row.get("replyAuthorFirstName")),
            toStr(row.get("replyAuthorLastName")),
            toBool(row.get("replyAuthorKnowsOriginalMessageAuthor"))
        )).toList();
      });
      resultReporter.report(results.size(), results, operation);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing Baseline Short Query 7", e);
    }
  }
}
