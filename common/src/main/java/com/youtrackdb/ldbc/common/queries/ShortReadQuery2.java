package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery2PersonPosts;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery2PersonPostsResult;

import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * IS2: Recent messages of a person
 *
 * Given a start Person, retrieve the last 10 Messages created by that user. For each Message, return
 * that Message, the original Post in its conversation, and the author of that Post. If any of the
 * Messages is a Post, then the original Post will be the same Message.
 */
public class ShortReadQuery2 extends ListQueryHandler<LdbcShortQuery2PersonPosts, LdbcShortQuery2PersonPostsResult> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcShortQuery2PersonPosts operation, GraphTraversalSource g) {
        return g.V()
                .has(PERSON, ID, operation.getPersonIdSQ2())
                .in(HAS_CREATOR)
                .order()
                    .by(CREATION_DATE, Order.desc)
                .limit(operation.getLimit())
                .as("message")
                .choose(
                        hasLabel(POST),
                        identity(),
                        repeat(out(REPLY_OF)).until(hasLabel(POST))
                )
                .as("originalPost")
                .out(HAS_CREATOR).as("author")
                .project("messageId", "messageContent", "messageCreationDate", "originalPostId",
                        "originalPostAuthorId", "originalPostAuthorFirstName", "originalPostAuthorLastName")
                    .by(select("message").values(ID))
                    .by(select("message").coalesce(values(IMAGE_FILE), values(CONTENT)))
                    .by(select("message").values(CREATION_DATE))
                    .by(select("originalPost").values(ID))
                    .by(select("author").values(ID))
                    .by(select("author").values(FIRST_NAME))
                    .by(select("author").values(LAST_NAME));
    }

    @Override
    protected LdbcShortQuery2PersonPostsResult toResult(Map<String, Object> record) {
        long messageId = getLong(record, "messageId");
        String messageContent = getString(record, "messageContent");
        long messageCreationDate = getDateAsMillis(record, "messageCreationDate");
        long originalPostId = getLong(record, "originalPostId");
        long originalPostAuthorId = getLong(record, "originalPostAuthorId");
        String originalPostAuthorFirstName = getString(record, "originalPostAuthorFirstName");
        String originalPostAuthorLastName = getString(record, "originalPostAuthorLastName");

        return new LdbcShortQuery2PersonPostsResult(
                messageId,
                messageContent,
                messageCreationDate,
                originalPostId,
                originalPostAuthorId,
                originalPostAuthorFirstName,
                originalPostAuthorLastName
        );
    }
}
