package com.youtrackdb.ldbc.ytdb.baseline;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery5MessageCreator;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery5MessageCreatorResult;

import static com.youtrackdb.ldbc.ytdb.baseline.SqlResultHelper.*;

public class BaselineShortQuery5
    implements OperationHandler<LdbcShortQuery5MessageCreator, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcShortQuery5MessageCreator operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var result = state.computeInTx(g -> {
        var row = querySingle(g, LdbcQuerySql.IS5,
            "messageId", operation.getMessageIdCreator());
        if (row == null) {
          throw new DbException(
              "No message found with ID: " + operation.getMessageIdCreator());
        }
        return new LdbcShortQuery5MessageCreatorResult(
            toLong(row.get("personId")),
            toStr(row.get("firstName")),
            toStr(row.get("lastName"))
        );
      });
      resultReporter.report(0, result, operation);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing Baseline Short Query 5", e);
    }
  }
}
