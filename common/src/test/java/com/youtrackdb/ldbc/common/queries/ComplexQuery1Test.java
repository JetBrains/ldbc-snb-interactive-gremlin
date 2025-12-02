package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery1;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery1Result;

import java.util.ArrayList;
import java.util.List;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * IC1: Transitive friends with a certain name
 *
 * Given a start Person, find Persons with a given first name that the start Person
 * is connected to (excluding start Person) by at most 3 steps via KNOWS relationships.
 *
 * Test graph:
 * <pre>
 *     Alice(1) --KNOWS-- Bob(2) --KNOWS-- Carol(3) --KNOWS-- David(4)
 *                          |                  |
 *                        Eve(5)            Frank(6) --KNOWS-- Grace(7)
 *
 * All "Carol" named: Carol(3)
 * All "Eve" named: Eve(5)
 * All "Grace" named: Grace(7) - but at distance 4 from Alice (should not be found)
 * </pre>
 */
class ComplexQuery1Test extends AbstractQueryTest {

    private static final long EVE_ID = 5L;
    private static final long FRANK_ID = 6L;
    private static final long GRACE_ID = 7L;

    private static final long PARIS_ID = 104L;
    private static final long FRANCE_ID = 105L;

    private static final long GOOGLE_ID = 602L;
    private static final long STANFORD_ID = 603L;

    @BeforeEach
    void setUpAdditionalData() {
        TestDataBuilder builder = builder();

        // Add more places
        Vertex paris = builder.createPlace(PARIS_ID, "Paris", "City");
        Vertex france = builder.createPlace(FRANCE_ID, "France", "Country");
        paris.addEdge(IS_PART_OF, france);

        // Add more organisations
        Vertex google = builder.createOrganisation(GOOGLE_ID, "Google", "Company");
        Vertex stanford = builder.createOrganisation(STANFORD_ID, "Stanford", "University");

        Vertex usa = g.V().has(PLACE, ID, USA_ID).next();
        google.addEdge(IS_LOCATED_IN, usa);
        stanford.addEdge(IS_LOCATED_IN, usa);

        // Add Eve (friend of Bob, distance 2 from Alice)
        Vertex eve = builder.createPerson(EVE_ID, "Eve", "Edwards", "female", paris);
        eve.addEdge(WORK_AT, google, WORK_FROM, 2018);

        // Add Frank (friend of Carol, distance 3 from Alice)
        Vertex frank = builder.createPerson(FRANK_ID, "Frank", "Foster", "male", paris);
        frank.addEdge(STUDY_AT, stanford, CLASS_YEAR, 2015);

        // Add Grace (friend of Frank, distance 4 from Alice - should NOT be found)
        Vertex grace = builder.createPerson(GRACE_ID, "Grace", "Green", "female", paris);

        // Create KNOWS relationships (bidirectional)
        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();
        Vertex carol = g.V().has(PERSON, ID, CAROL_ID).next();

        builder.createKnowsRelationship(bob, eve, DATE_2021);
        builder.createKnowsRelationship(carol, frank, DATE_2021);
        builder.createKnowsRelationship(frank, grace, DATE_2022);
    }

    @Test
    void testFindFriendAtDistance1() {
        ComplexReadQuery1 query = new ComplexReadQuery1();
        // Find friends of Alice named "Bob" (distance 1)
        LdbcQuery1 operation = new LdbcQuery1(ALICE_ID, "Bob", 10);

        List<LdbcQuery1Result> results = executeQuery(query, operation);

        assertEquals(1, results.size());
        LdbcQuery1Result bob = results.getFirst();
        assertEquals(BOB_ID, bob.getFriendId());
        assertEquals("Brown", bob.getFriendLastName());
        assertEquals(1, bob.getDistanceFromPerson());
    }

    @Test
    void testFindFriendAtDistance2() {
        ComplexReadQuery1 query = new ComplexReadQuery1();
        // Find friends named "Carol" (distance 2: Alice -> Bob -> Carol)
        LdbcQuery1 operation = new LdbcQuery1(ALICE_ID, "Carol", 10);

        List<LdbcQuery1Result> results = executeQuery(query, operation);

        assertEquals(1, results.size());
        LdbcQuery1Result carol = results.getFirst();
        assertEquals(CAROL_ID, carol.getFriendId());
        assertEquals("Clark", carol.getFriendLastName());
        assertEquals(2, carol.getDistanceFromPerson());
    }

    @Test
    void testFindFriendAtDistance2_alternativePath() {
        ComplexReadQuery1 query = new ComplexReadQuery1();
        // Find friends named "Eve" (distance 2: Alice -> Bob -> Eve)
        LdbcQuery1 operation = new LdbcQuery1(ALICE_ID, "Eve", 10);

        List<LdbcQuery1Result> results = executeQuery(query, operation);

        assertEquals(1, results.size());
        LdbcQuery1Result eve = results.getFirst();
        assertEquals(EVE_ID, eve.getFriendId());
        assertEquals("Edwards", eve.getFriendLastName());
        assertEquals(2, eve.getDistanceFromPerson());
        assertEquals("Paris", eve.getFriendCityName());

        // Verify work information
        List<LdbcQuery1Result.Organization> companies = toList(eve.getFriendCompanies());
        assertEquals(1, companies.size());
        assertEquals("Google", companies.getFirst().getOrganizationName());
        assertEquals(2018, companies.getFirst().getYear());
    }

    @Test
    void testFindFriendAtDistance3() {
        ComplexReadQuery1 query = new ComplexReadQuery1();
        // Find friends named "Frank" (distance 3: Alice -> Bob -> Carol -> Frank)
        LdbcQuery1 operation = new LdbcQuery1(ALICE_ID, "Frank", 10);

        List<LdbcQuery1Result> results = executeQuery(query, operation);

        assertEquals(1, results.size());
        LdbcQuery1Result frank = results.getFirst();
        assertEquals(FRANK_ID, frank.getFriendId());
        assertEquals("Foster", frank.getFriendLastName());
        assertEquals(3, frank.getDistanceFromPerson());

        // Verify university information
        List<LdbcQuery1Result.Organization> universities = toList(frank.getFriendUniversities());
        assertEquals(1, universities.size());
        assertEquals("Stanford", universities.getFirst().getOrganizationName());
        assertEquals(2015, universities.getFirst().getYear());
    }

    @Test
    void testFriendBeyondDistance3NotFound() {
        ComplexReadQuery1 query = new ComplexReadQuery1();
        // Grace is at distance 4 (Alice -> Bob -> Carol -> Frank -> Grace)
        // Should NOT be found as max distance is 3
        LdbcQuery1 operation = new LdbcQuery1(ALICE_ID, "Grace", 10);

        List<LdbcQuery1Result> results = executeQuery(query, operation);

        assertTrue(results.isEmpty(), "Grace at distance 4 should not be found");
    }

    @Test
    void testNoMatchingName() {
        ComplexReadQuery1 query = new ComplexReadQuery1();
        LdbcQuery1 operation = new LdbcQuery1(ALICE_ID, "Zoe", 10);

        List<LdbcQuery1Result> results = executeQuery(query, operation);

        assertTrue(results.isEmpty());
    }

    @Test
    void testOrderingByDistanceThenLastNameThenId() {
        TestDataBuilder builder = builder();
        ComplexReadQuery1 query = new ComplexReadQuery1();
        // Find all friends named with 'a' in various distances - use "David" who is at distance 3
        // We'll search for a name that matches multiple people at different distances

        // First, let's add another person named "Alex" at distance 2
        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();
        Vertex newYork = g.V().has(PLACE, ID, NEW_YORK_ID).next();

        // Note: Using "David" as first name and "Adams" as last name to test ordering
        Vertex alex = builder.createPerson(100L, "David", "Adams", "male", newYork);
        builder.createKnowsRelationship(bob, alex, DATE_2021);

        LdbcQuery1 operation = new LdbcQuery1(ALICE_ID, "David", 10);
        List<LdbcQuery1Result> results = executeQuery(query, operation);

        assertEquals(2, results.size());
        // First should be David Adams (distance 2, lastName "Adams")
        assertEquals(100L, results.getFirst().getFriendId());
        assertEquals("Adams", results.get(0).getFriendLastName());
        assertEquals(2, results.get(0).getDistanceFromPerson());

        // Second should be David Davis (distance 3, lastName "Davis")
        assertEquals(DAVID_ID, results.get(1).getFriendId());
        assertEquals("Davis", results.get(1).getFriendLastName());
        assertEquals(3, results.get(1).getDistanceFromPerson());
    }

    @Test
    void testLimitResults() {
        TestDataBuilder builder = builder();
        ComplexReadQuery1 query = new ComplexReadQuery1();
        // Add multiple people with same name at same distance
        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();
        Vertex newYork = g.V().has(PLACE, ID, NEW_YORK_ID).next();

        for (int i = 0; i < 5; i++) {
            Vertex person = builder.createPerson(200L + i, "Test", "Person" + i, "male", newYork);
            builder.createKnowsRelationship(bob, person, DATE_2021);
        }

        LdbcQuery1 operation = new LdbcQuery1(ALICE_ID, "Test", 3); // Limit to 3
        List<LdbcQuery1Result> results = executeQuery(query, operation);

        assertEquals(3, results.size(), "Should be limited to 3 results");
    }

    private <T> List<T> toList(Iterable<T> iterable) {
        List<T> list = new ArrayList<>();
        iterable.forEach(list::add);
        return list;
    }
}
