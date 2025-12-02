package com.youtrackdb.ldbc.common.queries;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

abstract class ListQueryHandler<TOperation extends Operation<List<TResult>>, TResult>
        implements OperationHandler<TOperation, TinkerPopConnectionState> {

    @Override
    public void executeOperation(TOperation operation, TinkerPopConnectionState state, ResultReporter resultReporter) throws DbException {
        try {
            Map<String, String> properties = state.getProperties();
            List<TResult> results = state.computeInTx(g -> {
                GraphTraversal<?, Map<String, Object>> traversal = buildTraversal(operation, g, properties);
                List<TResult> list = new ArrayList<>();
                while (traversal.hasNext()) {
                    list.add(toResult(traversal.next()));
                }
                return list;
            });
            resultReporter.report(results.size(), results, operation);
        } catch (DbException e) {
            throw e;
        } catch (Exception e) {
            throw new DbException("Error executing query", e);
        }
    }

    protected GraphTraversal<?, Map<String, Object>> buildTraversal(TOperation operation, GraphTraversalSource g, Map<String, String> properties) {
        return buildTraversal(operation, g);
    }

    protected abstract GraphTraversal<?, Map<String, Object>> buildTraversal(TOperation operation, GraphTraversalSource g);

    protected abstract TResult toResult(Map<String, Object> record);
}
