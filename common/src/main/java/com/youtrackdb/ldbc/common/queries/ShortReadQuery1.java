package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery1PersonProfile;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery1PersonProfileResult;

import java.util.Map;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static com.youtrackdb.ldbc.common.GremlinHelpers.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

/**
 * IS1: Profile of a person
 *
 * Given a start Person, retrieve their first name, last name, birthday, IP address, browser, and city
 * of residence.
 */
public class ShortReadQuery1 extends SingleResultQueryHandler<LdbcShortQuery1PersonProfile, LdbcShortQuery1PersonProfileResult> {
    @Override
    protected GraphTraversal<?, Map<String, Object>> buildTraversal(LdbcShortQuery1PersonProfile operation, GraphTraversalSource g) {
        return g.V()
                .has(PERSON, ID, operation.getPersonIdSQ1())
                .project(FIRST_NAME, LAST_NAME, BIRTHDAY, LOCATION_IP, BROWSER_USED, "cityId", GENDER, CREATION_DATE)
                    .by(FIRST_NAME)
                    .by(LAST_NAME)
                    .by(BIRTHDAY)
                    .by(LOCATION_IP)
                    .by(BROWSER_USED)
                    .by(out(IS_LOCATED_IN).values(ID))
                    .by(GENDER)
                    .by(CREATION_DATE);
    }

    @Override
    protected LdbcShortQuery1PersonProfileResult toResult(Map<String, Object> record) {
        return new LdbcShortQuery1PersonProfileResult(
                getString(record, FIRST_NAME),
                getString(record, LAST_NAME),
                getDateAsMillis(record, BIRTHDAY),
                getString(record, LOCATION_IP),
                getString(record, BROWSER_USED),
                getLong(record, "cityId"),
                getString(record, GENDER),
                getDateAsMillis(record, CREATION_DATE)
        );
    }
}
