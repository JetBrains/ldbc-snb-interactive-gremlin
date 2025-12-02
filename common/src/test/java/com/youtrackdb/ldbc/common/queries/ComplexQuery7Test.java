package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery7;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery7Result;

import java.util.Date;
import java.util.List;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * IC7: Recent likers
 *
 * Given a start Person, find the most recent likes on any of start Person's Messages.
 * Find Persons that liked any of start Person's Messages, the Messages they liked most
 * recently, the creation date of that like, and the latency in minutes between creation
 * of Messages and like. Additionally, return a flag indicating whether the liker is a
 * friend of start Person.
 */
class ComplexQuery7Test extends AbstractQueryTest {

    private static final long CAROL_LIKE_TIME = 1643702400000L; // 2022-02-01
    private static final long DAVID_LIKE_TIME = 1646121600000L; // 2022-03-01

    @BeforeEach
    void setUpAdditionalData() {
        Vertex alice = g.V().has(PERSON, ID, ALICE_ID).next();
        Vertex carol = g.V().has(PERSON, ID, CAROL_ID).next();
        Vertex david = g.V().has(PERSON, ID, DAVID_ID).next();
        Vertex post1 = g.V().has(POST, ID, POST1_ID).next();
        Vertex post2 = g.V().has(POST, ID, POST2_ID).next();

        // Carol likes Alice's post1 (Carol is not a direct friend of Alice)
        carol.addEdge(LIKES, post1, CREATION_DATE, new Date(CAROL_LIKE_TIME));

        // David likes Alice's post2 (David is also not a direct friend)
        david.addEdge(LIKES, post2, CREATION_DATE, new Date(DAVID_LIKE_TIME));
    }

    @Test
    void testFindRecentLikers() {
        ComplexReadQuery7 query = new ComplexReadQuery7();
        LdbcQuery7 operation = new LdbcQuery7(ALICE_ID, 10);

        List<LdbcQuery7Result> results = executeQuery(query, operation);

        // Should find Bob, Carol, David (all liked Alice's posts)
        assertFalse(results.isEmpty(), "Should find likers");

        // Bob liked post1 in fixture
        boolean foundBob = results.stream().anyMatch(r -> r.getPersonId() == BOB_ID);
        assertTrue(foundBob, "Bob should be found (liked post1)");

        // Carol liked post1
        boolean foundCarol = results.stream().anyMatch(r -> r.getPersonId() == CAROL_ID);
        assertTrue(foundCarol, "Carol should be found (liked post1)");
    }

    @Test
    void testOrderingByLikeDateDescThenPersonIdAsc() {
        ComplexReadQuery7 query = new ComplexReadQuery7();
        LdbcQuery7 operation = new LdbcQuery7(ALICE_ID, 10);

        List<LdbcQuery7Result> results = executeQuery(query, operation);

        // Verify ordering
        for (int i = 0; i < results.size() - 1; i++) {
            LdbcQuery7Result current = results.get(i);
            LdbcQuery7Result next = results.get(i + 1);

            if (current.getLikeCreationDate() == next.getLikeCreationDate()) {
                assertTrue(current.getPersonId() < next.getPersonId(),
                        "Same like date should be ordered by personId ASC");
            } else {
                assertTrue(current.getLikeCreationDate() > next.getLikeCreationDate(),
                        "Results should be ordered by like date DESC");
            }
        }
    }

    @Test
    void testFriendFlagIsCorrect() {
        ComplexReadQuery7 query = new ComplexReadQuery7();
        LdbcQuery7 operation = new LdbcQuery7(ALICE_ID, 10);

        List<LdbcQuery7Result> results = executeQuery(query, operation);

        // Bob is a direct friend of Alice
        LdbcQuery7Result bobResult = results.stream()
                .filter(r -> r.getPersonId() == BOB_ID)
                .findFirst()
                .orElse(null);

        assertNotNull(bobResult, "Should find Bob");
        assertFalse(bobResult.getIsNew(), "Bob should be marked as friend (isNew=false)");

        // Carol is not a direct friend (FoF)
        LdbcQuery7Result carolResult = results.stream()
                .filter(r -> r.getPersonId() == CAROL_ID)
                .findFirst()
                .orElse(null);

        assertNotNull(carolResult, "Should find Carol");
        assertTrue(carolResult.getIsNew(), "Carol should be marked as not-friend (isNew=true)");
    }

    @Test
    void testLatencyCalculation() {
        ComplexReadQuery7 query = new ComplexReadQuery7();
        LdbcQuery7 operation = new LdbcQuery7(ALICE_ID, 10);

        List<LdbcQuery7Result> results = executeQuery(query, operation);

        // All results should have non-negative latency
        for (LdbcQuery7Result result : results) {
            assertTrue(result.getMinutesLatency() >= 0,
                    "Latency should be non-negative (like must be after message creation)");
        }
    }

    @Test
    void testResultContainsCorrectFields() {
        ComplexReadQuery7 query = new ComplexReadQuery7();
        LdbcQuery7 operation = new LdbcQuery7(ALICE_ID, 10);

        List<LdbcQuery7Result> results = executeQuery(query, operation);

        assertFalse(results.isEmpty());

        LdbcQuery7Result result = results.getFirst();
        assertNotNull(result.getPersonFirstName(), "First name should not be null");
        assertNotNull(result.getPersonLastName(), "Last name should not be null");
        assertTrue(result.getMessageId() > 0, "Message ID should be positive");
        assertNotNull(result.getMessageContent(), "Message content should not be null");
    }

    @Test
    void testLimitResults() {
        ComplexReadQuery7 query = new ComplexReadQuery7();
        LdbcQuery7 operation = new LdbcQuery7(ALICE_ID, 2);

        List<LdbcQuery7Result> results = executeQuery(query, operation);

        assertEquals(2, results.size(), "Should be limited to 2 results");
    }

    @Test
    void testOnlyMostRecentLikePerPerson() {
        // Add another like from Carol on post2 (more recent)
        Vertex carol = g.V().has(PERSON, ID, CAROL_ID).next();
        Vertex post2 = g.V().has(POST, ID, POST2_ID).next();
        carol.addEdge(LIKES, post2, CREATION_DATE, new Date(1648800000000L)); // 2022-04-01 (after CAROL_LIKE_TIME)

        ComplexReadQuery7 query = new ComplexReadQuery7();
        LdbcQuery7 operation = new LdbcQuery7(ALICE_ID, 10);

        List<LdbcQuery7Result> results = executeQuery(query, operation);

        // Carol should appear only once with the most recent like
        long carolCount = results.stream().filter(r -> r.getPersonId() == CAROL_ID).count();
        assertEquals(1, carolCount, "Carol should appear only once (most recent like)");

        // And it should be for the later like (post2)
        LdbcQuery7Result carolResult = results.stream()
                .filter(r -> r.getPersonId() == CAROL_ID)
                .findFirst()
                .orElse(null);
        assertNotNull(carolResult);
        assertEquals(POST2_ID, carolResult.getMessageId(), "Should be Carol's most recent like (post2)");
    }

}
