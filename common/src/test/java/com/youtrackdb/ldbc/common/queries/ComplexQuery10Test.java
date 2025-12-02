package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery10;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery10Result;

import java.util.Date;
import java.util.List;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * IC10: Friend recommendation
 *
 * Given a start Person, find that Person's friends of friends (excluding the start Person
 * and immediate friends), who were born on or after the 21st of a given month and before
 * the 22nd of the following month. Calculate the similarity between each friend and the
 * start person based on common interests.
 */
class ComplexQuery10Test extends AbstractQueryTest {

    private static final long EVE_ID = 5L;

    @BeforeEach
    void setUpAdditionalData() {
        TestDataBuilder builder = builder();

        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();
        Vertex newYork = g.V().has(PLACE, ID, NEW_YORK_ID).next();
        Vertex tagJava = g.V().has(TAG, ID, TAG_JAVA_ID).next();

        // Eve is friend of Bob (FoF of Alice)
        // Birthday: Jan 25, 1990 (within Jan 21 - Feb 21 range for month=1)
        Vertex eve = builder.createPerson(EVE_ID, "Eve", "Edwards", "female",
                new Date(633225600000L), DATE_2020, newYork); // 1990-01-25
        builder.createKnowsRelationship(bob, eve, DATE_2021);

        // Eve's posts - one with Alice's interest (Java), one without
        Vertex evePost1 = builder.createPost(390L, "Eve Java post", DATE_2022, newYork, eve);
        evePost1.addEdge(HAS_TAG, tagJava);

        builder.createPost(391L, "Eve other post", DATE_2022, newYork, eve);
        // No tag matching Alice's interests
    }

    @Test
    void testQueryReturnsResults() {
        ComplexReadQuery10 query = new ComplexReadQuery10();
        // Month 1 (January) means looking for birthdays Jan 21 - Feb 21
        LdbcQuery10 operation = new LdbcQuery10(ALICE_ID, 1, 10);

        List<LdbcQuery10Result> results = executeQuery(query, operation);

        // Eve has birthday Jan 25 (in range Jan 21 - Feb 21)
        boolean foundEve = results.stream().anyMatch(r -> r.getPersonId() == EVE_ID);
        assertTrue(foundEve, "Eve (Jan 25 birthday) should be found for month=1");
    }

    @Test
    void testExcludesDirectFriends() {
        ComplexReadQuery10 query = new ComplexReadQuery10();
        LdbcQuery10 operation = new LdbcQuery10(ALICE_ID, 1, 10);

        List<LdbcQuery10Result> results = executeQuery(query, operation);

        // Bob is a direct friend, should not be included
        boolean foundBob = results.stream().anyMatch(r -> r.getPersonId() == BOB_ID);
        assertFalse(foundBob, "Direct friends should be excluded");
    }

    @Test
    void testExcludesStartPerson() {
        ComplexReadQuery10 query = new ComplexReadQuery10();
        LdbcQuery10 operation = new LdbcQuery10(ALICE_ID, 1, 10);

        List<LdbcQuery10Result> results = executeQuery(query, operation);

        boolean foundAlice = results.stream().anyMatch(r -> r.getPersonId() == ALICE_ID);
        assertFalse(foundAlice, "Start person should be excluded");
    }

    @Test
    void testOrderingByScoreDescThenIdAsc() {
        ComplexReadQuery10 query = new ComplexReadQuery10();
        LdbcQuery10 operation = new LdbcQuery10(ALICE_ID, 1, 10);

        List<LdbcQuery10Result> results = executeQuery(query, operation);

        // Verify ordering
        for (int i = 0; i < results.size() - 1; i++) {
            LdbcQuery10Result current = results.get(i);
            LdbcQuery10Result next = results.get(i + 1);

            if (current.getCommonInterestScore() == next.getCommonInterestScore()) {
                assertTrue(current.getPersonId() < next.getPersonId(),
                        "Same score should be ordered by personId ASC");
            } else {
                assertTrue(current.getCommonInterestScore() > next.getCommonInterestScore(),
                        "Results should be ordered by score DESC");
            }
        }
    }

    @Test
    void testResultContainsCorrectFields() {
        ComplexReadQuery10 query = new ComplexReadQuery10();
        LdbcQuery10 operation = new LdbcQuery10(ALICE_ID, 1, 10);

        List<LdbcQuery10Result> results = executeQuery(query, operation);

        if (!results.isEmpty()) {
            LdbcQuery10Result result = results.get(0);
            assertNotNull(result.getPersonFirstName(), "First name should not be null");
            assertNotNull(result.getPersonLastName(), "Last name should not be null");
            assertNotNull(result.getPersonGender(), "Gender should not be null");
            assertNotNull(result.getPersonCityName(), "City name should not be null");
        }
    }

    @Test
    void testLimitResults() {
        ComplexReadQuery10 query = new ComplexReadQuery10();
        LdbcQuery10 operation = new LdbcQuery10(ALICE_ID, 1, 1);

        List<LdbcQuery10Result> results = executeQuery(query, operation);

        assertTrue(results.size() <= 1, "Should be limited to at most 1 result");
    }

}
