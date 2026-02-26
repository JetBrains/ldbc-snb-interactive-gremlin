package com.youtrackdb.ldbc.ytdb.baseline;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery6MessageForum;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery6MessageForumResult;

import static com.youtrackdb.ldbc.ytdb.baseline.SqlResultHelper.*;

public class BaselineShortQuery6
    implements OperationHandler<LdbcShortQuery6MessageForum, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcShortQuery6MessageForum operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var result = state.computeInTx(g -> {
        var row = querySingle(g, LdbcQuerySql.IS6,
            "messageId", operation.getMessageForumId());
        if (row == null) {
          throw new DbException(
              "No message/forum found for ID: " + operation.getMessageForumId());
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
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing Baseline Short Query 6", e);
    }
  }
}
