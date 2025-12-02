package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery2;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery2Result;

import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * IC2: Recent messages by your friends
 *
 * Given a start Person, find the most recent Messages from all of that Person's friends.
 * Only consider Messages created before the given maxDate (excluding that day).
 */
public class ComplexReadQuery2 extends ListQueryHandler<LdbcQuery2, LdbcQuery2Result> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery2 operation, GraphTraversalSource g) {
        return g.V()
                .has(PERSON, ID, operation.getPersonIdQ2())
                    .out(KNOWS).as("friend")
                        .in(HAS_CREATOR)
                        .hasLabel(POST, COMMENT)
                        .has(CREATION_DATE, lt(operation.getMaxDate()))
                .order()
                    .by(CREATION_DATE, Order.desc)
                    .by(ID, Order.asc)
                .limit(operation.getLimit())
                .project("personId", FIRST_NAME, LAST_NAME, "messageId", "content", CREATION_DATE)
                    .by(select("friend").values(ID))
                    .by(select("friend").values(FIRST_NAME))
                    .by(select("friend").values(LAST_NAME))
                    .by(ID)
                    .by(coalesce(values(IMAGE_FILE), values(CONTENT)))
                    .by(CREATION_DATE);
    }

    @Override
    protected LdbcQuery2Result toResult(Map<String, Object> record) {
        long personId = getLong(record, "personId");
        String personFirstName = getString(record, FIRST_NAME);
        String personLastName = getString(record, LAST_NAME);
        long messageId = getLong(record, "messageId");
        String messageContent = getString(record, "content");
        long messageCreationDate = getDateAsMillis(record, CREATION_DATE);

        return new LdbcQuery2Result(
                personId,
                personFirstName,
                personLastName,
                messageId,
                messageContent,
                messageCreationDate
        );
    }
}
