package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate8AddFriendship;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;

/**
 * Update 8: Add friendship
 *
 * Add a friendship edge (knows) between two Persons.
 */
public class Update8AddFriendship extends UpdateHandler<LdbcUpdate8AddFriendship> {

    @Override
    protected void executeUpdate(LdbcUpdate8AddFriendship operation, GraphTraversalSource g) {
        g.V().has(PERSON, ID, operation.getPerson1Id()).as("p1")
                .V().has(PERSON, ID, operation.getPerson2Id()).as("p2")
                .addE(KNOWS).from("p1").to("p2").property(CREATION_DATE, operation.getCreationDate())
                .addE(KNOWS).from("p2").to("p1").property(CREATION_DATE, operation.getCreationDate())
                .iterate();
    }
}
