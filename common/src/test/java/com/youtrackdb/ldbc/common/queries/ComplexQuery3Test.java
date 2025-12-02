package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery3;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery3Result;

import java.util.Date;
import java.util.List;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * IC3: Friends and friends of friends that have been to given countries
 *
 * Given a start Person, find Persons that are their friends and friends of friends (excluding
 * the start Person) that have made Posts/Comments in both of the given Countries within a
 * given time period. Only Persons that are foreign to these Countries are considered.
 *
 * Test graph:
 * <pre>
 * Persons:
 *   - Alice(1) in USA (start person)
 *   - Bob(2) in UK
 *   - Eve(5) in France (foreign to Spain/Germany) - friend of Bob (distance 2 from Alice)
 *   - Frank(6) in France (foreign to Spain/Germany) - friend of Bob (distance 2 from Alice)
 *
 * Places:
 *   - Madrid(110) in Spain(111)
 *   - Berlin(112) in Germany(113)
 *   - Paris(104) in France(105)
 *
 * Posts/Comments:
 *   - Eve: 2 posts in Spain, 1 post in Germany (total: 3)
 *   - Frank: 1 post in Spain, 2 posts in Germany (total: 3)
 *
 * Expected results for query from Alice with Spain/Germany:
 *   - Eve and Frank both qualify (foreign to both countries, have posts in both)
 *   - Both have count=3, so ordered by personId ASC: Eve(5), Frank(6)
 * </pre>
 */
class ComplexQuery3Test extends AbstractQueryTest {

    // Date range for testing
    private static final Date START_DATE = new Date(1640995200000L); // 2022-01-01
    private static final int DURATION_DAYS = 365;

    // Additional Person IDs
    private static final long EVE_ID = 5L;
    private static final long FRANK_ID = 6L;

    // Additional Place IDs
    private static final long PARIS_ID = 104L;
    private static final long FRANCE_ID = 105L;
    private static final long MADRID_ID = 110L;
    private static final long SPAIN_ID = 111L;
    private static final long BERLIN_ID = 112L;
    private static final long GERMANY_ID = 113L;

    // Post IDs
    private static final long EVE_POST_SPAIN_1_ID = 310L;
    private static final long EVE_POST_SPAIN_2_ID = 311L;
    private static final long EVE_POST_GERMANY_ID = 312L;
    private static final long FRANK_POST_SPAIN_ID = 320L;
    private static final long FRANK_POST_GERMANY_1_ID = 321L;
    private static final long FRANK_POST_GERMANY_2_ID = 322L;

    @BeforeEach
    void setUpAdditionalData() {
        TestDataBuilder builder = builder();

        // Create additional places
        Vertex paris = builder.createPlace(PARIS_ID, "Paris", "City");
        Vertex france = builder.createPlace(FRANCE_ID, "France", "Country");
        paris.addEdge(IS_PART_OF, france);

        Vertex madrid = builder.createPlace(MADRID_ID, "Madrid", "City");
        Vertex spain = builder.createPlace(SPAIN_ID, "Spain", "Country");
        madrid.addEdge(IS_PART_OF, spain);

        Vertex berlin = builder.createPlace(BERLIN_ID, "Berlin", "City");
        Vertex germany = builder.createPlace(GERMANY_ID, "Germany", "Country");
        berlin.addEdge(IS_PART_OF, germany);

        // Create Eve and Frank who live in France (foreign to Spain and Germany)
        Vertex eve = builder.createPerson(EVE_ID, "Eve", "Edwards", "male", paris);
        Vertex frank = builder.createPerson(FRANK_ID, "Frank", "Foster", "male", paris);

        // Connect Eve and Frank to Bob (so they are distance 2 from Alice)
        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();
        builder.createKnowsRelationship(bob, eve, DATE_2021);
        builder.createKnowsRelationship(bob, frank, DATE_2021);

        // Create posts for Eve: 2 in Spain, 1 in Germany
        // Note: Posts are located in countries directly (not cities) per LDBC spec
        Date postDate = new Date(1645000000000L); // 2022-02-16 (within the date range)

        builder.createPost(EVE_POST_SPAIN_1_ID, "Eve in Spain 1", postDate, spain, eve);
        builder.createPost(EVE_POST_SPAIN_2_ID, "Eve in Spain 2", postDate, spain, eve);
        builder.createPost(EVE_POST_GERMANY_ID, "Eve in Germany", postDate, germany, eve);

        // Create posts for Frank: 1 in Spain, 2 in Germany
        builder.createPost(FRANK_POST_SPAIN_ID, "Frank in Spain", postDate, spain, frank);
        builder.createPost(FRANK_POST_GERMANY_1_ID, "Frank in Germany 1", postDate, germany, frank);
        builder.createPost(FRANK_POST_GERMANY_2_ID, "Frank in Germany 2", postDate, germany, frank);
    }

    @Test
    void testFindsFriendsWithPostsInBothCountries() {
        ComplexReadQuery3 query = new ComplexReadQuery3();
        LdbcQuery3 operation = new LdbcQuery3(ALICE_ID, "Spain", "Germany", START_DATE, DURATION_DAYS, 10);

        List<LdbcQuery3Result> results = executeQuery(query, operation);

        assertEquals(2, results.size(), "Should find Eve and Frank");

        // Both have count=3, so ordered by personId ASC
        LdbcQuery3Result eve = results.getFirst();
        assertEquals(EVE_ID, eve.getPersonId());
        assertEquals("Eve", eve.getPersonFirstName());
        assertEquals("Edwards", eve.getPersonLastName());
        assertEquals(2, eve.getxCount(), "Eve has 2 posts in Spain");
        assertEquals(1, eve.getyCount(), "Eve has 1 post in Germany");
        assertEquals(3, eve.getCount(), "Eve total count is 3");

        LdbcQuery3Result frank = results.get(1);
        assertEquals(FRANK_ID, frank.getPersonId());
        assertEquals("Frank", frank.getPersonFirstName());
        assertEquals("Foster", frank.getPersonLastName());
        assertEquals(1, frank.getxCount(), "Frank has 1 post in Spain");
        assertEquals(2, frank.getyCount(), "Frank has 2 posts in Germany");
        assertEquals(3, frank.getCount(), "Frank total count is 3");
    }

    @Test
    void testExcludesPersonsLivingInQueriedCountries() {
        TestDataBuilder builder = builder();

        // Create a person living in Spain (not foreign to Spain)
        Vertex madrid = g.V().has(PLACE, ID, MADRID_ID).next();
        Vertex spain = g.V().has(PLACE, ID, SPAIN_ID).next();
        Vertex germany = g.V().has(PLACE, ID, GERMANY_ID).next();
        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();

        Vertex george = builder.createPerson(7L, "George", "Garcia", "male", madrid);
        builder.createKnowsRelationship(bob, george, DATE_2021);

        // George has posts in both countries but lives in Spain
        Date postDate = new Date(1645000000000L);
        builder.createPost(330L, "George in Spain", postDate, spain, george);
        builder.createPost(331L, "George in Germany", postDate, germany, george);

        ComplexReadQuery3 query = new ComplexReadQuery3();
        LdbcQuery3 operation = new LdbcQuery3(ALICE_ID, "Spain", "Germany", START_DATE, DURATION_DAYS, 10);

        List<LdbcQuery3Result> results = executeQuery(query, operation);

        // George should NOT be in results because he lives in Spain
        assertTrue(results.stream().noneMatch(r -> r.getPersonId() == 7L),
                "George should be excluded because he lives in Spain (not foreign)");
        assertEquals(2, results.size(), "Only Eve and Frank should be in results");
    }

    @Test
    void testExcludesPersonsWithPostsInOnlyOneCountry() {
        TestDataBuilder builder = builder();

        // Create a person with posts only in Spain (not in Germany)
        Vertex paris = g.V().has(PLACE, ID, PARIS_ID).next();
        Vertex spain = g.V().has(PLACE, ID, SPAIN_ID).next();
        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();

        Vertex henry = builder.createPerson(8L, "Henry", "Hall", "male", paris);
        builder.createKnowsRelationship(bob, henry, DATE_2021);

        // Henry only has posts in Spain
        Date postDate = new Date(1645000000000L);
        builder.createPost(340L, "Henry in Spain", postDate, spain, henry);

        ComplexReadQuery3 query = new ComplexReadQuery3();
        LdbcQuery3 operation = new LdbcQuery3(ALICE_ID, "Spain", "Germany", START_DATE, DURATION_DAYS, 10);

        List<LdbcQuery3Result> results = executeQuery(query, operation);

        // Henry should NOT be in results because he has no posts in Germany
        assertTrue(results.stream().noneMatch(r -> r.getPersonId() == 8L),
                "Henry should be excluded because he has no posts in Germany");
    }

    @Test
    void testDateRangeFiltering() {
        ComplexReadQuery3 query = new ComplexReadQuery3();
        // Use a date range that doesn't include our posts (posts are from 2022-02-16)
        Date oldStartDate = new Date(1577836800000L); // 2020-01-01
        int shortDuration = 30; // Only 30 days

        LdbcQuery3 operation = new LdbcQuery3(ALICE_ID, "Spain", "Germany", oldStartDate, shortDuration, 10);

        List<LdbcQuery3Result> results = executeQuery(query, operation);

        assertTrue(results.isEmpty(), "Should have no results when date range doesn't include posts");
    }

    @Test
    void testOrderingByCountDescThenIdAsc() {
        TestDataBuilder builder = builder();

        // Create another person with higher count
        Vertex paris = g.V().has(PLACE, ID, PARIS_ID).next();
        Vertex spain = g.V().has(PLACE, ID, SPAIN_ID).next();
        Vertex germany = g.V().has(PLACE, ID, GERMANY_ID).next();
        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();

        Vertex ivan = builder.createPerson(9L, "Ivan", "Ivanov", "male", paris);
        builder.createKnowsRelationship(bob, ivan, DATE_2021);

        // Ivan has 3 posts in Spain, 3 posts in Germany (total: 6)
        Date postDate = new Date(1645000000000L);
        builder.createPost(350L, "Ivan in Spain 1", postDate, spain, ivan);
        builder.createPost(351L, "Ivan in Spain 2", postDate, spain, ivan);
        builder.createPost(352L, "Ivan in Spain 3", postDate, spain, ivan);
        builder.createPost(353L, "Ivan in Germany 1", postDate, germany, ivan);
        builder.createPost(354L, "Ivan in Germany 2", postDate, germany, ivan);
        builder.createPost(355L, "Ivan in Germany 3", postDate, germany, ivan);

        ComplexReadQuery3 query = new ComplexReadQuery3();
        LdbcQuery3 operation = new LdbcQuery3(ALICE_ID, "Spain", "Germany", START_DATE, DURATION_DAYS, 10);

        List<LdbcQuery3Result> results = executeQuery(query, operation);

        assertEquals(3, results.size());

        // First should be Ivan with count=6
        assertEquals(9L, results.get(0).getPersonId());
        assertEquals(6, results.get(0).getCount());

        // Then Eve and Frank with count=3, ordered by personId
        assertEquals(EVE_ID, results.get(1).getPersonId());
        assertEquals(3, results.get(1).getCount());

        assertEquals(FRANK_ID, results.get(2).getPersonId());
        assertEquals(3, results.get(2).getCount());
    }

    @Test
    void testLimitResults() {
        ComplexReadQuery3 query = new ComplexReadQuery3();
        LdbcQuery3 operation = new LdbcQuery3(ALICE_ID, "Spain", "Germany", START_DATE, DURATION_DAYS, 1);

        List<LdbcQuery3Result> results = executeQuery(query, operation);

        assertEquals(1, results.size(), "Should be limited to 1 result");
        assertEquals(EVE_ID, results.getFirst().getPersonId(), "First result should be Eve (lowest ID among tied counts)");
    }

    @Test
    void testNoResultsForNonExistentCountry() {
        ComplexReadQuery3 query = new ComplexReadQuery3();
        LdbcQuery3 operation = new LdbcQuery3(ALICE_ID, "NonExistent1", "NonExistent2",
                START_DATE, DURATION_DAYS, 10);

        List<LdbcQuery3Result> results = executeQuery(query, operation);

        assertTrue(results.isEmpty(), "Should have no results for non-existent countries");
    }
}
