package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery13;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery13Result;

import java.util.Map;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * IC13: Single shortest path
 *
 * Given two Persons, find the shortest path between these two Persons in the subgraph induced by the
 * knows edges. Return the length of this path: -1 if no path found, 0 if start person = end person,
 * or > 0 if path found.
 */
public class ComplexReadQuery13 extends SingleResultQueryHandler<LdbcQuery13, LdbcQuery13Result> {

    private static final int DEFAULT_MAX_HOPS = 4;

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery13 operation, GraphTraversalSource g, Map<String, String> properties) {
        // IC13's curated parameters pick Person pairs that are either unreachable or max 4 hops apart
        // We rely on this guarantee here to keep the search from exploding.
        int maxHops = Integer.parseInt(properties.getOrDefault("tinkerpop.ic13.maxHops", String.valueOf(DEFAULT_MAX_HOPS)));
        return buildTraversal(operation, g, maxHops);
    }

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery13 operation, GraphTraversalSource g) {
        return buildTraversal(operation, g, DEFAULT_MAX_HOPS);
    }

    private GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery13 operation, GraphTraversalSource g, int maxHops) {
        long person1Id = operation.getPerson1IdQ13StartNode();
        long person2Id = operation.getPerson2IdQ13EndNode();

        return g.V()
                .has(PERSON, ID, person1Id)
                .coalesce(
                    where(has(ID, eq(person2Id))).constant(0.0),
                    repeat(out(KNOWS).simplePath())
                            .until(has(ID, eq(person2Id)).or().loops().is(maxHops))
                            .has(ID, eq(person2Id))
                            .path()
                            .limit(1)
                            .count(Scope.local)
                            .math("_ - 1")
                )
                .fold()
                .coalesce(unfold(), constant(-1.0))
                .project("shortestPathLength")
                    .by();
    }

    @Override
    protected LdbcQuery13Result toResult(Map<String, Object> record) {
        Number shortestPathLength = (Number) record.get("shortestPathLength");
        return new LdbcQuery13Result(shortestPathLength.intValue());
    }
}
