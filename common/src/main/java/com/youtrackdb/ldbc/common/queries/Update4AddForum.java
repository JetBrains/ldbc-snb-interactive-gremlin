package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate4AddForum;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;

/**
 * Update 4: Add forum
 *
 * Add a Forum node, connected to the network by 2 possible edge types (hasModerator, hasTag).
 */
public class Update4AddForum extends UpdateHandler<LdbcUpdate4AddForum> {

    @Override
    protected void executeUpdate(LdbcUpdate4AddForum operation, GraphTraversalSource g) {
        var traversal = g.addV(FORUM)
                .property(ID, operation.getForumId())
                .property(TITLE, operation.getForumTitle())
                .property(CREATION_DATE, operation.getCreationDate())
                .as("forum")
                .addE(HAS_MODERATOR)
                .from("forum")
                .to(V().has(PERSON, ID, operation.getModeratorPersonId()));

        for (Long tagId : operation.getTagIds()) {
            traversal.addE(HAS_TAG).from("forum").to(V().has(TAG, ID, tagId));
        }

        traversal.iterate();
    }
}
