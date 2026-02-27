package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery2PersonPosts;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery2PersonPostsResult;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

public class SqlShortQuery2
    implements OperationHandler<LdbcShortQuery2PersonPosts, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcShortQuery2PersonPosts operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var results = state.computeInTx(g -> {
        var rows = query(g, LdbcQuerySql.IS2,
            "personId", operation.getPersonIdSQ2(),
            "limit", operation.getLimit());
        return rows.stream().map(row -> new LdbcShortQuery2PersonPostsResult(
            toLong(row.get("messageId")),
            toStr(row.get("messageContent")),
            toDateMillis(row.get("messageCreationDate")),
            toLong(row.get("originalPostId")),
            toLong(row.get("originalPostAuthorId")),
            toStr(row.get("originalPostAuthorFirstName")),
            toStr(row.get("originalPostAuthorLastName"))
        )).toList();
      });
      resultReporter.report(results.size(), results, operation);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing Baseline Short Query 2", e);
    }
  }
}
