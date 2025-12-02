package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery12;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery12Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * IC12: Expert search
 *
 * Given a start Person, find the Comments that this Person's friends made in reply to Posts, considering
 * only those Comments that are direct (single-hop) replies to Posts. Only consider Posts with a Tag in
 * a given TagClass or in a descendent of that TagClass. Count the number of these reply Comments, and
 * collect the Tags. Return Persons with at least one reply, the reply count, and the collection of Tags.
 */
public class ComplexReadQuery12 extends ListQueryHandler<LdbcQuery12, LdbcQuery12Result> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery12 operation, GraphTraversalSource g) {
        return g.V().has(PERSON, ID, operation.getPersonIdQ12())
                .out(KNOWS).as("friend")
                    .in(HAS_CREATOR).hasLabel(COMMENT)
                        .out(REPLY_OF).hasLabel(POST)
                            .out(HAS_TAG)
                .where(out(HAS_TYPE)
                        .emit()
                        .repeat(out(IS_SUBCLASS_OF))
                        .has(NAME, operation.getTagClassName())
                )
                .group()
                    .by(select("friend"))
                    .by(fold())
                .unfold()
                .project("personId", FIRST_NAME, LAST_NAME, "tagNames", "replyCount")
                    .by(select(Column.keys).values(ID))
                    .by(select(Column.keys).values(FIRST_NAME))
                    .by(select(Column.keys).values(LAST_NAME))
                    .by(select(Column.values).unfold().values(NAME).dedup().fold())
                    .by(select(Column.values).count(Scope.local))
                .order()
                    .by("replyCount", Order.desc)
                    .by("personId", Order.asc)
                .limit(operation.getLimit());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected LdbcQuery12Result toResult(Map<String, Object> record) {
        long personId = getLong(record, "personId");
        String personFirstName = getString(record, FIRST_NAME);
        String personLastName = getString(record, LAST_NAME);
        List<String> tagNames = (List<String>) record.getOrDefault("tagNames", new ArrayList<>());
        int replyCount = getInt(record, "replyCount");

        return new LdbcQuery12Result(
                personId,
                personFirstName,
                personLastName,
                tagNames,
                replyCount
        );
    }
}
