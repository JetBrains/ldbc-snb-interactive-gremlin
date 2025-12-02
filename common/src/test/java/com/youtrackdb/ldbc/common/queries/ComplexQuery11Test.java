package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery11;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery11Result;

import java.util.List;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * IC11: Job referral
 *
 * Given a start Person, find that Person's friends and friends of friends (excluding start
 * Person) who started working in some Company in a given Country before a given date
 * (workFromYear).
 */
class ComplexQuery11Test extends AbstractQueryTest {

    private static final long GOOGLE_ID = 610L;
    private static final long APPLE_ID = 611L;

    @BeforeEach
    void setUpAdditionalData() {
        TestDataBuilder builder = builder();
        Vertex usa = g.V().has(PLACE, ID, USA_ID).next();

        // Create companies in USA
        Vertex google = builder.createOrganisation(GOOGLE_ID, "Google", "Company");
        google.addEdge(IS_LOCATED_IN, usa);

        Vertex apple = builder.createOrganisation(APPLE_ID, "Apple", "Company");
        apple.addEdge(IS_LOCATED_IN, usa);

        // Bob works at Google since 2015 (in USA) - Bob is distance 1 from Alice
        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();
        bob.addEdge(WORK_AT, google, WORK_FROM, 2015);

        // Carol works at Apple since 2018 (in USA) - Carol is distance 2 from Alice
        Vertex carol = g.V().has(PERSON, ID, CAROL_ID).next();
        carol.addEdge(WORK_AT, apple, WORK_FROM, 2018);
    }

    @Test
    void testFindFriendsWorkingInCountryBeforeYear() {
        ComplexReadQuery11 query = new ComplexReadQuery11();
        // Find friends working in USA before 2020
        LdbcQuery11 operation = new LdbcQuery11(ALICE_ID, "United States", 2020, 10);

        List<LdbcQuery11Result> results = executeQuery(query, operation);

        // Should find Bob (Google, 2015) and Carol (Apple, 2018)
        assertFalse(results.isEmpty(), "Should find friends working in USA");

        boolean foundBob = results.stream().anyMatch(r -> r.getPersonId() == BOB_ID);
        boolean foundCarol = results.stream().anyMatch(r -> r.getPersonId() == CAROL_ID);

        assertTrue(foundBob, "Bob should be found (Google, 2015)");
        assertTrue(foundCarol, "Carol should be found (Apple, 2018)");
    }

    @Test
    void testWorkFromYearFilter() {
        ComplexReadQuery11 query = new ComplexReadQuery11();
        // Find friends working in USA before 2016 (excludes Carol who started in 2018)
        LdbcQuery11 operation = new LdbcQuery11(ALICE_ID, "United States", 2016, 10);

        List<LdbcQuery11Result> results = executeQuery(query, operation);

        boolean foundBob = results.stream().anyMatch(r -> r.getPersonId() == BOB_ID);
        boolean foundCarol = results.stream().anyMatch(r -> r.getPersonId() == CAROL_ID);

        assertTrue(foundBob, "Bob should be found (started 2015 < 2016)");
        assertFalse(foundCarol, "Carol should NOT be found (started 2018 >= 2016)");
    }

    @Test
    void testCountryFilter() {
        ComplexReadQuery11 query = new ComplexReadQuery11();
        // Find friends working in UK before 2020 - no companies in UK
        LdbcQuery11 operation = new LdbcQuery11(ALICE_ID, "United Kingdom", 2020, 10);

        List<LdbcQuery11Result> results = executeQuery(query, operation);

        // No companies in UK, so no results
        assertTrue(results.isEmpty(), "Should have no results for UK (no companies there)");
    }

    @Test
    void testOrderingByYearAscThenIdAscThenOrgNameDesc() {
        ComplexReadQuery11 query = new ComplexReadQuery11();
        LdbcQuery11 operation = new LdbcQuery11(ALICE_ID, "United States", 2020, 10);

        List<LdbcQuery11Result> results = executeQuery(query, operation);

        // Verify ordering by year ASC
        for (int i = 0; i < results.size() - 1; i++) {
            LdbcQuery11Result current = results.get(i);
            LdbcQuery11Result next = results.get(i + 1);

            assertTrue(current.getOrganizationWorkFromYear() <= next.getOrganizationWorkFromYear(),
                    "Results should be ordered by work year ASC");
        }
    }

    @Test
    void testResultContainsCorrectFields() {
        ComplexReadQuery11 query = new ComplexReadQuery11();
        LdbcQuery11 operation = new LdbcQuery11(ALICE_ID, "United States", 2020, 10);

        List<LdbcQuery11Result> results = executeQuery(query, operation);

        LdbcQuery11Result bobResult = results.stream()
                .filter(r -> r.getPersonId() == BOB_ID)
                .findFirst()
                .orElse(null);

        assertNotNull(bobResult, "Should find Bob");
        assertEquals("Bob", bobResult.getPersonFirstName());
        assertEquals("Brown", bobResult.getPersonLastName());
        assertEquals("Google", bobResult.getOrganizationName());
        assertEquals(2015, bobResult.getOrganizationWorkFromYear());
    }

    @Test
    void testLimitResults() {
        ComplexReadQuery11 query = new ComplexReadQuery11();
        LdbcQuery11 operation = new LdbcQuery11(ALICE_ID, "United States", 2020, 1);

        List<LdbcQuery11Result> results = executeQuery(query, operation);

        assertEquals(1, results.size(), "Should be limited to 1 result");
    }

    @Test
    void testNoResultsForFutureYear() {
        ComplexReadQuery11 query = new ComplexReadQuery11();
        // Find friends who started working before 2000 (none)
        LdbcQuery11 operation = new LdbcQuery11(ALICE_ID, "United States", 2000, 10);

        List<LdbcQuery11Result> results = executeQuery(query, operation);

        assertTrue(results.isEmpty(), "No one worked before 2000");
    }
}
