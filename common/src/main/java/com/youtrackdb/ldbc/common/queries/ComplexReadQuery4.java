package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery4;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery4Result;

import java.util.Date;
import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * IC4: New topics
 *
 * Given a start Person, find Tags that are attached to Posts that were created by that Person's friends.
 * Only include Tags that were attached to friends' Posts created within a given time interval and that
 * were never attached to friends' Posts created before this interval.
 */
public class ComplexReadQuery4 extends ListQueryHandler<LdbcQuery4, LdbcQuery4Result> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery4 operation, GraphTraversalSource g) {
        Date startDate = operation.getStartDate();
        Date endDate = plusDays(operation.getStartDate(), operation.getDurationDays());

        return g.V()
                .has(PERSON, ID, operation.getPersonIdQ4())
                .out(KNOWS).aggregate("friends")
                    .in(HAS_CREATOR)
                        .hasLabel(POST)
                        .has(CREATION_DATE, lt(startDate))
                        .out(HAS_TAG)
                        .dedup()
                        .aggregate("priorTags")
                .limit(1) // keep a single traverser
                .select("friends").unfold()
                    .in(HAS_CREATOR)
                        .hasLabel(POST)
                        .has(CREATION_DATE, between(startDate, endDate))
                        .out(HAS_TAG)
                        .where(without("priorTags"))
                .values(NAME)
                .groupCount()
                .unfold()
                .order()
                    .by(Column.values, Order.desc)
                    .by(Column.keys, Order.asc)
                .limit(operation.getLimit())
                .project("tagName", "postCount")
                    .by(select(Column.keys))
                    .by(select(Column.values));
    }

    @Override
    protected LdbcQuery4Result toResult(Map<String, Object> record) {
        String tagName = getString(record, "tagName");
        int postCount = getInt(record, "postCount");

        return new LdbcQuery4Result(tagName, postCount);
    }
}
