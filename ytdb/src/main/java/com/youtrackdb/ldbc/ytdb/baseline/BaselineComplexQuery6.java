package com.youtrackdb.ldbc.ytdb.baseline;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery6;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery6Result;

import static com.youtrackdb.ldbc.ytdb.baseline.SqlResultHelper.*;

/**
 * IC6: Tag co-occurrence.
 * Find other Tags that co-occur with a given Tag on Posts by friends/fof.
 */
public class BaselineComplexQuery6
    implements OperationHandler<LdbcQuery6, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery6 op,
      TinkerPopConnectionState state,
      ResultReporter rr) throws DbException {
    try {
      var results = state.computeInTx(g -> {
        var rows = query(g, LdbcQuerySql.IC6,
            "personId", op.getPersonIdQ6(),
            "tagName", op.getTagName(),
            "limit", op.getLimit());
        return rows.stream().map(row -> new LdbcQuery6Result(
            toStr(row.get("tagName")),
            toInt(row.get("postCount"))
        )).toList();
      });
      rr.report(results.size(), results, op);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 6", e);
    }
  }
}
