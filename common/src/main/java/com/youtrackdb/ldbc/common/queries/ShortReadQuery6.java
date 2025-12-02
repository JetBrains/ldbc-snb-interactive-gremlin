package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery6MessageForum;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery6MessageForumResult;

import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * IS6: Forum of a message
 *
 * Given a Message, retrieve the Forum that contains it and the Person that moderates that Forum.
 * Since Comments are not directly contained in Forums, for Comments, return the Forum containing
 * the original Post in the thread which the Comment is replying to.
 */
public class ShortReadQuery6 extends SingleResultQueryHandler<LdbcShortQuery6MessageForum, LdbcShortQuery6MessageForumResult> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcShortQuery6MessageForum operation, GraphTraversalSource g) {
        return g.V()
                .hasLabel(POST, COMMENT)
                .has(ID, operation.getMessageForumId())
                .choose(
                    hasLabel(POST),
                    in(CONTAINER_OF).hasLabel(FORUM),
                    repeat(out(REPLY_OF)).until(hasLabel(POST)).in(CONTAINER_OF).hasLabel(FORUM)
                )
                .as("forum")
                .out(HAS_MODERATOR).as("moderator")
                .project("forumId", "forumTitle", "moderatorId", "moderatorFirstName", "moderatorLastName")
                    .by(select("forum").values(ID))
                    .by(select("forum").values(TITLE))
                    .by(select("moderator").values(ID))
                    .by(select("moderator").values(FIRST_NAME))
                    .by(select("moderator").values(LAST_NAME));
    }

    @Override
    protected LdbcShortQuery6MessageForumResult toResult(Map<String, Object> record) {
        long forumId = getLong(record, "forumId");
        String forumTitle = getString(record, "forumTitle");
        long moderatorId = getLong(record, "moderatorId");
        String moderatorFirstName = getString(record, "moderatorFirstName");
        String moderatorLastName = getString(record, "moderatorLastName");

        return new LdbcShortQuery6MessageForumResult(
                forumId,
                forumTitle,
                moderatorId,
                moderatorFirstName,
                moderatorLastName
        );
    }
}
