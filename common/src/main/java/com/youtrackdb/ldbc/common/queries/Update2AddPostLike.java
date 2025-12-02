package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate2AddPostLike;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;

/**
 * Update 2: Add like to post
 *
 * Add a likes edge to a Post.
 */
public class Update2AddPostLike extends UpdateHandler<LdbcUpdate2AddPostLike> {

    @Override
    protected void executeUpdate(LdbcUpdate2AddPostLike operation, GraphTraversalSource g) {
        g.addE(LIKES)
                .from(V().has(PERSON, ID, operation.getPersonId()))
                .to(V().has(POST, ID, operation.getPostId()))
                .property(CREATION_DATE, operation.getCreationDate())
                .iterate();
    }
}
