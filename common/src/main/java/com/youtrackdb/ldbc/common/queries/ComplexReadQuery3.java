package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery3;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery3Result;

import java.util.Date;
import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * IC3: Friends and friends of friends that have been to given countries
 *
 * Given a start Person, find Persons that are their friends and friends of friends (excluding the start
 * Person) that have made Posts/Comments in both of the given Countries within a given time period.
 * Only Persons that are foreign to these Countries are considered.
 */
public class ComplexReadQuery3 extends ListQueryHandler<LdbcQuery3, LdbcQuery3Result> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery3 operation, GraphTraversalSource g) {
        Date startDate = operation.getStartDate();
        Date endDate = plusDays(startDate, operation.getDurationDays());

        return g.V()
                .has(PERSON, ID, operation.getPersonIdQ3())
                    .repeat(out(KNOWS).simplePath()).times(2).emit()
                .dedup()
                .where(
                    out(IS_LOCATED_IN)
                        .out(IS_PART_OF)
                        .values(NAME)
                        .is(without(operation.getCountryXName(), operation.getCountryYName()))
                )
                .project(ID, FIRST_NAME, LAST_NAME, "xCount", "yCount")
                    .by(ID)
                    .by(FIRST_NAME)
                    .by(LAST_NAME)
                    .by(in(HAS_CREATOR)
                            .hasLabel(POST, COMMENT)
                            .has(CREATION_DATE, between(startDate, endDate))
                            .where(out(IS_LOCATED_IN).has(PLACE, NAME, operation.getCountryXName()))
                            .count())
                    .by(in(HAS_CREATOR)
                            .hasLabel(POST, COMMENT)
                            .has(CREATION_DATE, between(startDate, endDate))
                            .where(out(IS_LOCATED_IN).has(PLACE, NAME, operation.getCountryYName()))
                            .count())
                .where(and(
                    select("xCount").is(gt(0)),
                    select("yCount").is(gt(0))
                ))
                .project("personId", FIRST_NAME, LAST_NAME, "xCount", "yCount", "count")
                    .by(ID)
                    .by(FIRST_NAME)
                    .by(LAST_NAME)
                    .by("xCount")
                    .by("yCount")
                    .by(math("xCount + yCount"))
                .order()
                    .by("count", Order.desc)
                    .by("personId", Order.asc)
                .limit(operation.getLimit());
    }

    @Override
    protected LdbcQuery3Result toResult(Map<String, Object> record) {
        long personId = getLong(record, "personId");
        String personFirstName = getString(record, FIRST_NAME);
        String personLastName = getString(record, LAST_NAME);
        int xCount = getInt(record, "xCount");
        int yCount = getInt(record, "yCount");
        int count = getInt(record, "count");

        return new LdbcQuery3Result(
                personId,
                personFirstName,
                personLastName,
                xCount,
                yCount,
                count
        );
    }
}
