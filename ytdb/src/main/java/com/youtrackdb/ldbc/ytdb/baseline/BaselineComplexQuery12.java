package com.youtrackdb.ldbc.ytdb.baseline;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery12;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery12Result;

import static com.youtrackdb.ldbc.ytdb.baseline.SqlResultHelper.*;

/**
 * IC12: Expert search.
 * Find friends' Comments replying to Posts tagged with a given TagClass
 * (or descendant). The SQL returns set(tag.name) as tagNames.
 */
public class BaselineComplexQuery12
    implements OperationHandler<LdbcQuery12, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery12 op,
      TinkerPopConnectionState state,
      ResultReporter rr) throws DbException {
    try {
      var results = state.computeInTx(g -> {
        var rows = query(g, LdbcQuerySql.IC12,
            "personId", op.getPersonIdQ12(),
            "tagClassName", op.getTagClassName(),
            "limit", op.getLimit());
        return rows.stream().map(row -> new LdbcQuery12Result(
            toLong(row.get("personId")),
            toStr(row.get("firstName")),
            toStr(row.get("lastName")),
            toStringList(row.get("tagNames")),
            toInt(row.get("replyCount"))
        )).toList();
      });
      rr.report(results.size(), results, op);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 12", e);
    }
  }
}
