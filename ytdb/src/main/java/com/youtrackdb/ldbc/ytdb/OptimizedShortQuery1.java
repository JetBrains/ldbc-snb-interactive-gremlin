package com.youtrackdb.ldbc.ytdb;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery1PersonProfile;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery1PersonProfileResult;

import java.util.Map;

import static com.jetbrains.youtrackdb.api.gremlin.__.out;
import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static com.youtrackdb.ldbc.common.GremlinHelpers.*;

/**
 * Default Gremlin implementation for LDBC Short Query 1: Person Profile.
 * Given a person, retrieve their profile information.
 */
public class OptimizedShortQuery1 implements OperationHandler<LdbcShortQuery1PersonProfile, TinkerPopConnectionState> {

    @Override
    public void executeOperation(
            LdbcShortQuery1PersonProfile operation,
            TinkerPopConnectionState state,
            ResultReporter resultReporter) throws DbException {
        try {
            GraphTraversal<?, Map<String, Object>> traversal = buildTraversal(state, operation);

            if (traversal.hasNext()) {
                Map<String, Object> record = traversal.next();
                LdbcShortQuery1PersonProfileResult result = toResult(record);
                resultReporter.report(0, result, operation);
            } else {
                throw new DbException("No person found with ID: " + operation.getPersonIdSQ1());
            }
        } catch (Exception e) {
            throw new DbException("Error executing Short Query 1", e);
        }
    }

    protected GraphTraversal<?, Map<String, Object>> buildTraversal(
            TinkerPopConnectionState state,
            LdbcShortQuery1PersonProfile operation) throws Exception {

        return state.computeInTx(g -> g.V()
                .has(PERSON, ID, operation.getPersonIdSQ1())
                .project(
                        FIRST_NAME, LAST_NAME, BIRTHDAY,
                        LOCATION_IP, BROWSER_USED, "cityId",
                        GENDER, CREATION_DATE
                )
                .by(FIRST_NAME)
                .by(LAST_NAME)
                .by(BIRTHDAY)
                .by(LOCATION_IP)
                .by(BROWSER_USED)
                .by(out(IS_LOCATED_IN).values(ID))
                .by(GENDER)
                .by(CREATION_DATE));
    }

    protected LdbcShortQuery1PersonProfileResult toResult(Map<String, Object> record) {
        String firstName = getString(record, FIRST_NAME);
        String lastName = getString(record, LAST_NAME);
        long birthday = getDateAsMillis(record, BIRTHDAY);
        String locationIP = getString(record, LOCATION_IP);
        String browserUsed = getString(record, BROWSER_USED);
        long cityId = getLong(record, "cityId");
        String gender = getString(record, GENDER);
        long creationDate = getDateAsMillis(record, CREATION_DATE);

        return new LdbcShortQuery1PersonProfileResult(
                firstName,
                lastName,
                birthday,
                locationIP,
                browserUsed,
                cityId,
                gender,
                creationDate
        );
    }
}
