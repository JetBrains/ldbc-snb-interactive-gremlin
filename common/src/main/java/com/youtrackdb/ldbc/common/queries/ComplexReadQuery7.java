package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery7;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery7Result;

import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * IC7: Recent likers
 *
 * Given a start Person, find the most recent likes on any of start Person's Messages. Find Persons that
 * liked any of start Person's Messages, the Messages they liked most recently, the creation date of that
 * like, and the latency in minutes between creation of Messages and like. Additionally, return a flag
 * indicating whether the liker is a friend of start Person.
 */
public class ComplexReadQuery7 extends ListQueryHandler<LdbcQuery7, LdbcQuery7Result> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery7 operation, GraphTraversalSource g) {
        return g.V()
                .has(PERSON, ID, operation.getPersonIdQ7()).as("startPerson")
                    .in(HAS_CREATOR).as("message")
                        .inE(LIKES).as("like").outV().as("liker")
                .project("personId", FIRST_NAME, LAST_NAME, "likeCreationDate",
                        "messageId", "messageContent", "messageCreationDate", "isNew")
                    .by(ID)
                    .by(FIRST_NAME)
                    .by(LAST_NAME)
                    .by(select("like").values(CREATION_DATE))
                    .by(select("message").values(ID))
                    .by(select("message").coalesce(
                            values(IMAGE_FILE),
                            values(CONTENT)
                    ))
                    .by(select("message").values(CREATION_DATE))
                    .by(coalesce(
                            out(KNOWS).where(eq("startPerson")).constant(false),
                            constant(true)
                    ))
                .group()
                    .by("personId")
                    .by(order()
                            .by("likeCreationDate", Order.desc)
                            .by("messageId", Order.asc)
                            .limit(1)
                    )
                .select(Column.values).<Map<String, Object>>unfold()
                .order()
                    .by("likeCreationDate", Order.desc)
                    .by("personId", Order.asc)
                .limit(operation.getLimit());
    }

    @Override
    protected LdbcQuery7Result toResult(Map<String, Object> record) {
        long personId = getLong(record, "personId");
        String personFirstName = getString(record, FIRST_NAME);
        String personLastName = getString(record, LAST_NAME);
        long likeCreationDate = getDateAsMillis(record, "likeCreationDate");
        long messageId = getLong(record, "messageId");
        String messageContent = getString(record, "messageContent");
        long messageCreationDate = getDateAsMillis(record, "messageCreationDate");
        boolean isNew = getBoolean(record, "isNew");
        int latencyAsMilli = (int) ((likeCreationDate - messageCreationDate) / 60000);

        return new LdbcQuery7Result(
                personId,
                personFirstName,
                personLastName,
                likeCreationDate,
                messageId,
                messageContent,
                latencyAsMilli,
                isNew
        );
    }
}
