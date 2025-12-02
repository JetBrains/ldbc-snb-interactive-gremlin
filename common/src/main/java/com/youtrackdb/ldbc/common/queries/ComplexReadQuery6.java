package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery6;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery6Result;

import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;

/**
 * IC6: Tag co-occurrence
 *
 * Given a start Person and a Tag, find the other Tags that occur together with this Tag on Posts that
 * were created by start Person's friends and friends of friends (excluding start Person). Return top 10
 * Tags, and the count of Posts that contain both this Tag and the given Tag.
 */
public class ComplexReadQuery6 extends ListQueryHandler<LdbcQuery6, LdbcQuery6Result> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery6 operation, GraphTraversalSource g) {
        return g.V()
                .has(PERSON, ID, operation.getPersonIdQ6())
                .repeat(out(KNOWS).simplePath()).times(2).emit()
                .dedup()
                    .in(HAS_CREATOR)
                    .hasLabel(POST)
                    .where(out(HAS_TAG).has(NAME, operation.getTagName()))
                        .out(HAS_TAG)
                        .has(NAME, neq(operation.getTagName()))
                        .values(NAME)
                .groupCount()
                .order(Scope.local)
                    .by(Column.values, Order.desc)
                    .by(Column.keys, Order.asc)
                .limit(Scope.local, operation.getLimit())
                .unfold()
                .project("tagName", "postCount")
                    .by(select(Column.keys))
                    .by(select(Column.values));
    }

    @Override
    protected LdbcQuery6Result toResult(Map<String, Object> record) {
        String tagName = getString(record, "tagName");
        int postCount = getInt(record, "postCount");

        return new LdbcQuery6Result(tagName, postCount);
    }
}
