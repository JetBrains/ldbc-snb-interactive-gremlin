package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate6AddPost;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;

/**
 * Update 6: Add post
 *
 * Add a Post node connected to the network by 4 possible edge types (hasCreator, containerOf,
 * isLocatedIn, hasTag).
 */
public class Update6AddPost extends UpdateHandler<LdbcUpdate6AddPost> {

    @Override
    protected void executeUpdate(LdbcUpdate6AddPost operation, GraphTraversalSource g) {
        var traversal = g.addV(POST)
                        .property(ID, operation.getPostId())
                        .property(CREATION_DATE, operation.getCreationDate())
                        .property(LOCATION_IP, operation.getLocationIp())
                        .property(BROWSER_USED, operation.getBrowserUsed())
                        .property(LANGUAGE, operation.getLanguage())
                        .property(LENGTH, operation.getLength());

                if (operation.getImageFile() != null && !operation.getImageFile().isEmpty()) {
                    traversal = traversal.property(IMAGE_FILE, operation.getImageFile());
                } else {
                    traversal = traversal.property(CONTENT, operation.getContent());
                }

                traversal.as("post")
                        .addE(HAS_CREATOR)
                            .from("post")
                            .to(V().has(PERSON, ID, operation.getAuthorPersonId()))
                        .addE(CONTAINER_OF)
                            .from(V().has(FORUM, ID, operation.getForumId()))
                            .to("post")
                        .addE(IS_LOCATED_IN)
                            .from("post")
                            .to(V().has(PLACE, ID, operation.getCountryId()));

                for (Long tagId : operation.getTagIds()) {
                    traversal.addE(HAS_TAG).from("post").to(V().has(TAG, ID, tagId));
                }

        traversal.iterate();
    }
}
