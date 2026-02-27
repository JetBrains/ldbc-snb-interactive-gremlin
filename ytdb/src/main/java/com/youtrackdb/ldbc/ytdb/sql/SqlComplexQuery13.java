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
      LdbcQuery13 operation,
      TinkerPopConnectionState state,
      ResultReporter resultReporter) throws DbException {
    try {
      var result = state.computeInTx(graph -> {
        var row = querySingle(graph, LdbcQuerySql.IC13,
            "person1Id", operation.getPerson1IdQ13StartNode(),
            "person2Id", operation.getPerson2IdQ13EndNode());
        if (row == null) {
          return new LdbcQuery13Result(-1);
        }
        return new LdbcQuery13Result(toInt(row.get("pathLength")));
      });
      resultReporter.report(0, result, operation);
    } catch (Exception e) {
      throw new DbException("Error executing SQL Complex Query 13", e);
    }
  }
}
