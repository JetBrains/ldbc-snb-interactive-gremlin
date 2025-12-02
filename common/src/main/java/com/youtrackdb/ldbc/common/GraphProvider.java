package com.youtrackdb.ldbc.common;

import org.apache.commons.lang3.function.FailableConsumer;
import org.apache.commons.lang3.function.FailableFunction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.Closeable;

public interface GraphProvider extends Closeable {
    /**
     * Executes the given code block within a transaction.
     * The transaction is automatically committed on success and rolled back on exception.
     *
     * @param code The code to execute within the transaction
     * @param <E> Exception type that may be thrown
     * @throws E if the code block throws an exception
     */
    <E extends Exception> void executeInTx(FailableConsumer<GraphTraversalSource, E> code) throws Exception;

    /**
     * Executes the given code block within a transaction and returns a result.
     * The transaction is automatically committed on success and rolled back on exception.
     *
     * @param code The code to execute within the transaction
     * @param <E> Exception type that may be thrown
     * @param <R> Return type
     * @return The result of the code block execution
     * @throws E if the code block throws an exception
     */
    <E extends Exception, R> R computeInTx(FailableFunction<GraphTraversalSource, R, E> code) throws Exception;
}

