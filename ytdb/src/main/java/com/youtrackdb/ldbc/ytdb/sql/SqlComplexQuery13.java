package com.youtrackdb.ldbc.ytdb.sql;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery13;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery13Result;

import static com.youtrackdb.ldbc.ytdb.sql.SqlResultHelper.*;

/**
 * IC13: Single shortest path.
 * Returns the shortest path length between two Persons via KNOWS edges.
 * -1 if no path, 0 if same person, >0 otherwise.
 */
public class SqlComplexQuery13
    implements OperationHandler<LdbcQuery13, TinkerPopConnectionState> {

  @Override
  public void executeOperation(
      LdbcQuery13 op,
      TinkerPopConnectionState state,
      ResultReporter rr) throws DbException {
    try {
      var result = state.computeInTx(g -> {
        var row = querySingle(g, LdbcQuerySql.IC13,
            "person1Id", op.getPerson1IdQ13StartNode(),
            "person2Id", op.getPerson2IdQ13EndNode());
        if (row == null) {
          return new LdbcQuery13Result(-1);
        }
        return new LdbcQuery13Result(toInt(row.get("pathLength")));
      });
      rr.report(0, result, op);
    } catch (DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 13", e);
    }
  }
}
