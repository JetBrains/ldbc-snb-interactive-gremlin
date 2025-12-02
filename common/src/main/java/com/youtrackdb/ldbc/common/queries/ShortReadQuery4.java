package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery4MessageContent;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery4MessageContentResult;

import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * IS4: Content of a message
 *
 * Given a Message, retrieve its content and creation date.
 */
public class ShortReadQuery4 extends SingleResultQueryHandler<LdbcShortQuery4MessageContent, LdbcShortQuery4MessageContentResult> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcShortQuery4MessageContent operation, GraphTraversalSource g) {

        return g.V()
                .hasLabel(POST, COMMENT)
                .has(ID, operation.getMessageIdContent())
                .project("messageContent", CREATION_DATE)
                    .by(coalesce(values(IMAGE_FILE), values(CONTENT)))
                    .by(CREATION_DATE);
    }

    @Override
    protected LdbcShortQuery4MessageContentResult toResult(Map<String, Object> record) {
        String messageContent = getString(record, "messageContent");
        long messageCreationDate = getDateAsMillis(record, CREATION_DATE);

        return new LdbcShortQuery4MessageContentResult(
                messageContent,
                messageCreationDate
        );
    }
}
