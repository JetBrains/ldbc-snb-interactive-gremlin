package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery5;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery5Result;

import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * IC5: New groups
 *
 * Given a start Person, denote their friends and friends of friends (excluding the start Person) as
 * otherPerson. Find Forums that any Person otherPerson became a member of after a given date. For
 * each of those Forums, count the number of Posts that were created by the Person otherPerson.
 *
 * NOTE: We should group by postId.
 * NOTE 2: There might be duplicates (forums with the same name).
 * NOTE 3: We might end up with 0 "postCount" - that's fine
 */
public class ComplexReadQuery5 extends ListQueryHandler<LdbcQuery5, LdbcQuery5Result> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery5 operation, GraphTraversalSource g) {
        return g.V()
                .has(PERSON, ID, operation.getPersonIdQ5())
                .repeat(out(KNOWS).simplePath()).times(2).emit()
                .dedup().as("person")
                    .inE(HAS_MEMBER)
                    .has(JOIN_DATE, gt(operation.getMinDate()))
                    .outV()
                    .hasLabel(FORUM)
                .group()
                    .by()
                    .by(map(out(CONTAINER_OF)
                            .hasLabel(POST)
                            .out(HAS_CREATOR)
                            .where(eq("person"))
                            .count()
                    ).sum())
                .unfold()
                .order()
                    .by(Column.values, Order.desc)
                    .by(select(Column.keys).values(ID), Order.asc)
                .project("forumTitle", "postCount")
                    .by(select(Column.keys).values(TITLE))
                    .by(select(Column.values))
                .limit(operation.getLimit());
    }

    @Override
    protected LdbcQuery5Result toResult(Map<String, Object> record) {
        String forumTitle = getString(record, "forumTitle");
        int postCount = getInt(record, "postCount");

        return new LdbcQuery5Result(forumTitle, postCount);
    }
}
