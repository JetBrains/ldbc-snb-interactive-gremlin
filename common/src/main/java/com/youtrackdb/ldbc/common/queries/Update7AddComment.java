package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate7AddComment;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;

/**
 * Update 7: Add comment
 *
 * Add a Comment node replying to a Post/Comment, connected to the network by 4 possible edge types
 * (replyOf, hasCreator, isLocatedIn, hasTag).
 */
public class Update7AddComment extends UpdateHandler<LdbcUpdate7AddComment> {

    @Override
    protected void executeUpdate(LdbcUpdate7AddComment operation, GraphTraversalSource g) {
        var traversal = g.addV(COMMENT)
                        .property(ID, operation.getCommentId())
                        .property(CREATION_DATE, operation.getCreationDate())
                        .property(LOCATION_IP, operation.getLocationIp())
                        .property(BROWSER_USED, operation.getBrowserUsed())
                        .property(CONTENT, operation.getContent())
                        .property(LENGTH, operation.getLength())
                        .as("comment")
                        .addE(HAS_CREATOR)
                            .from("comment")
                            .to(V().has(PERSON, ID, operation.getAuthorPersonId()))
                        .addE(IS_LOCATED_IN)
                            .from("comment")
                            .to(V().has(PLACE, ID, operation.getCountryId()));

                if (operation.getReplyToPostId() != -1) {
                    traversal.addE(REPLY_OF).from("comment").to(V().has(POST, ID, operation.getReplyToPostId()));
                } else if (operation.getReplyToCommentId() != -1) {
                    traversal.addE(REPLY_OF).from("comment").to(V().has(COMMENT, ID, operation.getReplyToCommentId()));
                }

                for (Long tagId : operation.getTagIds()) {
                    traversal.addE(HAS_TAG).from("comment").to(V().has(TAG, ID, tagId));
                }

        traversal.iterate();
    }
}
