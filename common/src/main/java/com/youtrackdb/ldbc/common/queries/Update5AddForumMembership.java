package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate5AddForumMembership;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;

/**
 * Update 5: Add forum membership
 *
 * Add a Forum membership edge (hasMember) to a Person.
 */
public class Update5AddForumMembership extends UpdateHandler<LdbcUpdate5AddForumMembership> {

    @Override
    protected void executeUpdate(LdbcUpdate5AddForumMembership operation, GraphTraversalSource g) {
        g.V()
                .has(FORUM, ID, operation.getForumId())
                .addE(HAS_MEMBER)
                .to(V().has(PERSON, ID, operation.getPersonId()))
                .property(JOIN_DATE, operation.getJoinDate())
                .iterate();
    }
}
