package com.youtrackdb.ldbc.common.queries;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;

import java.util.Map;

public abstract class SingleResultQueryHandler<TOperation extends Operation<TOperationResult>, TOperationResult>
        implements OperationHandler<TOperation, TinkerPopConnectionState> {

    @Override
    public void executeOperation(TOperation operation, TinkerPopConnectionState state, ResultReporter resultReporter) throws DbException {
        try {
            Map<String, String> properties = state.getProperties();
            TOperationResult result = state.computeInTx(g -> {
                GraphTraversal<?, Map<String, Object>> traversal = buildTraversal(operation, g, properties);
                if (traversal.hasNext()) {
                    return toResult(traversal.next());
                } else {
                    throw new DbException("No results for query");
                }
            });
            resultReporter.report(0, result, operation);
        } catch (DbException e) {
            throw e;
        } catch (Exception e) {
            throw new DbException("Error executing query", e);
        }
    }

    protected GraphTraversal<?, Map<String, Object>> buildTraversal(TOperation operation, GraphTraversalSource g, Map<String, String> ignoredProperties) {
        return buildTraversal(operation, g);
    }

    protected abstract GraphTraversal<?, Map<String, Object>> buildTraversal(TOperation operation, GraphTraversalSource g);

    protected abstract TOperationResult toResult(Map<String, Object> record);
}
