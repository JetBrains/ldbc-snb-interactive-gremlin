package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery8;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery8Result;

import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;

/**
 * IC8: Recent replies
 *
 * Given a start Person, find the most recent Comments that are replies to Messages of the start Person.
 * Only consider direct (single-hop) replies, not the transitive (multi-hop) ones. Return the reply
 * Comments, and the Person that created each reply Comment.
 */
public class ComplexReadQuery8 extends ListQueryHandler<LdbcQuery8, LdbcQuery8Result> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery8 operation, GraphTraversalSource g) {
        return g.V()
                .has(PERSON, ID, operation.getPersonIdQ8())
                    .in(HAS_CREATOR)
                        .in(REPLY_OF)
                        .hasLabel(COMMENT)
                .order()
                    .by(CREATION_DATE, Order.desc)
                    .by(ID, Order.asc)
                .limit(operation.getLimit())
                .as("comment")
                    .out(HAS_CREATOR)
                .project("personId", FIRST_NAME, LAST_NAME, "commentCreationDate",
                        "commentId", "commentContent")
                    .by(ID)
                    .by(FIRST_NAME)
                    .by(LAST_NAME)
                    .by(select("comment").values(CREATION_DATE))
                    .by(select("comment").values(ID))
                    .by(select("comment").values(CONTENT));
    }

    @Override
    protected LdbcQuery8Result toResult(Map<String, Object> record) {
        long personId = getLong(record, "personId");
        String personFirstName = getString(record, FIRST_NAME);
        String personLastName = getString(record, LAST_NAME);
        long commentCreationDate = getDateAsMillis(record, "commentCreationDate");
        long commentId = getLong(record, "commentId");
        String commentContent = getString(record, "commentContent");

        return new LdbcQuery8Result(
                personId,
                personFirstName,
                personLastName,
                commentCreationDate,
                commentId,
                commentContent
        );
    }
}
