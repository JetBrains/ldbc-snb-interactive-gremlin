package com.youtrackdb.ldbc.common;

import org.apache.commons.lang3.function.FailableConsumer;
import org.apache.commons.lang3.function.FailableFunction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.DbConnectionState;

import java.io.IOException;
import java.util.Map;

public class TinkerPopConnectionState extends DbConnectionState {
    private final GraphProvider graphProvider;
    private final Map<String, String> properties;

    public TinkerPopConnectionState(GraphProvider graphProvider, Map<String, String> properties) {
        this.graphProvider = graphProvider;
        this.properties = properties;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Executes code within a transaction with automatic commit/rollback.
     * Use this for update operations that modify the graph.
     *
     * @param code The code to execute within the transaction
     * @param <E> Exception type that may be thrown
     * @throws E if the code block throws an exception
     */
    public <E extends Exception> void executeInTx(FailableConsumer<GraphTraversalSource, E> code) throws Exception {
        graphProvider.executeInTx(code);
    }

    /**
     * Executes code within a transaction and returns a result.
     * Use this for read operations that need transactional consistency.
     *
     * @param code The code to execute within the transaction
     * @param <E> Exception type that may be thrown
     * @param <R> Return type
     * @return The result of the code block execution
     * @throws E if the code block throws an exception
     */
    public <E extends Exception, R> R computeInTx(FailableFunction<GraphTraversalSource, R, E> code) throws Exception {
        return graphProvider.computeInTx(code);
    }

    @Override
    public void close() throws IOException {
        if (graphProvider != null) {
            graphProvider.close();
        }
    }
}
