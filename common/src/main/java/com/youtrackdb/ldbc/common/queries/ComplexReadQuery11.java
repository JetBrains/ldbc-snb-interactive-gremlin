package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery11;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery11Result;

import java.util.Map;

import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;

/**
 * IC11: Job referral
 *
 * Given a start Person, find that Person's friends and friends of friends (excluding start Person) who
 * started working in some Company in a given Country before a given date (workFromYear).
 */
public class ComplexReadQuery11 extends ListQueryHandler<LdbcQuery11, LdbcQuery11Result> {

    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcQuery11 operation, GraphTraversalSource g) {
        return g.V()
                .has(PERSON, ID, operation.getPersonIdQ11()).as("start")
                .repeat(out(KNOWS).simplePath()).times(2).emit()
                .dedup().as("person")
                    .outE(WORK_AT)
                    .has(WORK_FROM, lt(operation.getWorkFromYear())).as("workAt")
                    .inV().as("company")
                        .out(IS_LOCATED_IN)
                        .has(NAME, operation.getCountryName())
                .project("personId", FIRST_NAME, LAST_NAME,
                        "organizationName", "organizationWorkFromYear")
                    .by(select("person").values(ID))
                    .by(select("person").values(FIRST_NAME))
                    .by(select("person").values(LAST_NAME))
                    .by(select("company").values(NAME))
                    .by(select("workAt").values(WORK_FROM))
                .order()
                    .by("organizationWorkFromYear", Order.asc)
                    .by("personId", Order.asc)
                    .by("organizationName", Order.desc)
                .limit(operation.getLimit());
    }

    @Override
    protected LdbcQuery11Result toResult(Map<String, Object> record) {
        long personId = getLong(record, "personId");
        String personFirstName = getString(record, FIRST_NAME);
        String personLastName = getString(record, LAST_NAME);
        String organizationName = getString(record, "organizationName");
        int organizationWorkFromYear = getInt(record, "organizationWorkFromYear");

        return new LdbcQuery11Result(
                personId,
                personFirstName,
                personLastName,
                organizationName,
                organizationWorkFromYear
        );
    }
}
