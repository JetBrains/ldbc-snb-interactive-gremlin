package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery9;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery9Result;

import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.P.lt;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * IC9: Recent messages by friends or friends of friends
 *
 * Given a start Person, find the most recent Messages created by that Person's friends or friends of
 * friends (excluding the start Person). Only consider Messages created before the given maxDate
 * (excluding that day).
 */
public class ComplexReadQuery9 extends ListQueryHandler<LdbcQuery9, LdbcQuery9Result> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery9 operation, GraphTraversalSource g) {
        return g.V()
                .has(PERSON, ID, operation.getPersonIdQ9())
                .repeat(out(KNOWS).simplePath()).times(2).emit()
                .dedup().as("person")
                    .in(HAS_CREATOR)
                    .has(CREATION_DATE, lt(operation.getMaxDate()))
                .order()
                    .by(CREATION_DATE, Order.desc)
                    .by(ID, Order.asc)
                .limit(operation.getLimit())
                .project("personId", FIRST_NAME, LAST_NAME, "messageId",
                        "messageContent", "messageCreationDate")
                    .by(select("person").values(ID))
                    .by(select("person").values(FIRST_NAME))
                    .by(select("person").values(LAST_NAME))
                    .by(ID)
                    .by(coalesce(values(IMAGE_FILE), values(CONTENT)))
                    .by(CREATION_DATE);
    }

    @Override
    protected LdbcQuery9Result toResult(Map<String, Object> record) {
        long personId = getLong(record, "personId");
        String personFirstName = getString(record, FIRST_NAME);
        String personLastName = getString(record, LAST_NAME);
        long messageId = getLong(record, "messageId");
        String messageContent = getString(record, "messageContent");
        long messageCreationDate = getDateAsMillis(record, "messageCreationDate");

        return new LdbcQuery9Result(
                personId,
                personFirstName,
                personLastName,
                messageId,
                messageContent,
                messageCreationDate
        );
    }
}
