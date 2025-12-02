package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate3AddCommentLike;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;

/**
 * Update 3: Add like to comment
 *
 * Add a likes edge to a Comment.
 */
public class Update3AddCommentLike extends UpdateHandler<LdbcUpdate3AddCommentLike> {

    @Override
    protected void executeUpdate(LdbcUpdate3AddCommentLike operation, GraphTraversalSource g) {
        g.addE(LIKES)
                .from(V().has(PERSON, ID, operation.getPersonId()))
                .to(V().has(COMMENT, ID, operation.getCommentId()))
                .property(CREATION_DATE, operation.getCreationDate())
                .iterate();
    }
}
