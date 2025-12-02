package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery7MessageReplies;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery7MessageRepliesResult;

import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * IS7: Replies of a message
 *
 * Given a Message, retrieve the (1-hop) Comments that reply to it. In addition, return a boolean flag
 * indicating if the author of the reply knows the author of the original message. If author is same as
 * original author, return False for knows flag.
 */
public class ShortReadQuery7 extends ListQueryHandler<LdbcShortQuery7MessageReplies, LdbcShortQuery7MessageRepliesResult> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcShortQuery7MessageReplies operation, GraphTraversalSource g) {
        return g.V()
                .hasLabel(POST, COMMENT)
                .has(ID, operation.getMessageRepliesId())
                .as("message")
                    .out(HAS_CREATOR).as("author")
                .select("message")
                    .in(REPLY_OF).as("comment")
                        .out(HAS_CREATOR).as("replyAuthor")
                .project("commentId", "commentContent", "commentCreationDate",
                        "replyAuthorId", "replyAuthorFirstName", "replyAuthorLastName", "replyAuthorKnows")
                    .by(select("comment").values(ID))
                    .by(select("comment").coalesce(values(IMAGE_FILE), values(CONTENT)))
                    .by(select("comment").values(CREATION_DATE))
                    .by(select("replyAuthor").values(ID))
                    .by(select("replyAuthor").values(FIRST_NAME))
                    .by(select("replyAuthor").values(LAST_NAME))
                    .by(
                        select("replyAuthor")
                            .out(KNOWS)
                            .where(eq("author"))
                        .fold()
                        .coalesce(unfold().constant(true), constant(false))
                    )
                .order()
                    .by("commentCreationDate", Order.desc)
                    .by("replyAuthorId", Order.asc);
    }

    @Override
    protected LdbcShortQuery7MessageRepliesResult toResult(Map<String, Object> record) {
        long commentId = getLong(record, "commentId");
        String commentContent = getString(record, "commentContent");
        long commentCreationDate = getDateAsMillis(record, "commentCreationDate");
        long replyAuthorId = getLong(record, "replyAuthorId");
        String replyAuthorFirstName = getString(record, "replyAuthorFirstName");
        String replyAuthorLastName = getString(record, "replyAuthorLastName");
        boolean replyAuthorKnows = getBoolean(record, "replyAuthorKnows");

        return new LdbcShortQuery7MessageRepliesResult(
                commentId,
                commentContent,
                commentCreationDate,
                replyAuthorId,
                replyAuthorFirstName,
                replyAuthorLastName,
                replyAuthorKnows
        );
    }
}
