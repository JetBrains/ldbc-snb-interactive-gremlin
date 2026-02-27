package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery4MessageContent;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery4MessageContentResult;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

public class SqlShortQuery4
    implements OperationHandler<LdbcShortQuery4MessageContent, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcShortQuery4MessageContent operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var result = state.computeInTx(g -> {
        var row = querySingle(g, LdbcQuerySql.IS4,
            "messageId", operation.getMessageIdContent());
        if (row == null) {
          throw new DbException(
              "No message found with ID: " + operation.getMessageIdContent());
        }
        return new LdbcShortQuery4MessageContentResult(
            toStr(row.get("messageContent")),
            toDateMillis(row.get("creationDate"))
        );
      });
      resultReporter.report(0, result, operation);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing Baseline Short Query 4", e);
    }
  }
}
