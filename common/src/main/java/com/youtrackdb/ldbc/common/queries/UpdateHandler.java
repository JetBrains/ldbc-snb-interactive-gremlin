package com.youtrackdb.ldbc.common.queries;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;

abstract class UpdateHandler<TOperation extends Operation<LdbcNoResult>> implements OperationHandler<TOperation, TinkerPopConnectionState> {

    @Override
    public void executeOperation(TOperation operation, TinkerPopConnectionState state, ResultReporter resultReporter) throws DbException {
        try {
            state.executeInTx(g -> executeUpdate(operation, g));
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        } catch (Exception e) {
            throw new DbException("Error executing update", e);
        }
    }

    protected abstract void executeUpdate(TOperation operation, GraphTraversalSource g);
}
