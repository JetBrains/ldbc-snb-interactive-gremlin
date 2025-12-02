package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery9;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery9Result;

import java.util.Date;
import java.util.List;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * IC9: Recent messages by friends or friends of friends
 *
 * Given a start Person, find the most recent Messages created by that Person's
 * friends or friends of friends (excluding the start Person). Only consider
 * Messages created before the given maxDate (excluding that day).
 *
 * Test validates:
 * - Messages from direct friends are included
 * - Messages from friends-of-friends are included
 * - Messages from start person are excluded
 * - Messages after maxDate are excluded
 * - Results are ordered by creationDate DESC, then ID ASC
 */
class ComplexQuery9Test extends AbstractQueryTest {

    // Additional message IDs
    private static final long POST_BOB_1_ID = 350L;
    private static final long POST_BOB_2_ID = 351L;
    private static final long COMMENT_CAROL_ID = 450L;
    private static final long POST_DAVID_ID = 360L;

    // Dates
    private static final Date DATE_JAN_15 = new Date(1642204800000L); // 2022-01-15
    private static final Date DATE_FEB_01 = new Date(1643702400000L); // 2022-02-01
    private static final Date DATE_FEB_15 = new Date(1644883200000L); // 2022-02-15
    private static final Date DATE_MAR_01 = new Date(1646121600000L); // 2022-03-01
    private static final Date DATE_MAR_15 = new Date(1647302400000L); // 2022-03-15

    @BeforeEach
    void setUpAdditionalData() {
        TestDataBuilder builder = builder();

        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();
        Vertex carol = g.V().has(PERSON, ID, CAROL_ID).next();
        Vertex david = g.V().has(PERSON, ID, DAVID_ID).next();
        Vertex newYork = g.V().has(PLACE, ID, NEW_YORK_ID).next();
        Vertex techTalk = g.V().has(FORUM, ID, TECH_TALK_ID).next();

        // Bob's posts (direct friend of Alice)
        Vertex postBob1 = builder.createPost(POST_BOB_1_ID, "Bob post January", DATE_JAN_15, newYork, bob);
        techTalk.addEdge(CONTAINER_OF, postBob1);

        Vertex postBob2 = builder.createPost(POST_BOB_2_ID, "Bob post February", DATE_FEB_15, newYork, bob);
        techTalk.addEdge(CONTAINER_OF, postBob2);

        // Carol's comment (friend-of-friend via Bob)
        Vertex commentCarol = builder.createComment(COMMENT_CAROL_ID, "Carol's comment", DATE_FEB_01, newYork, carol);
        commentCarol.addEdge(REPLY_OF, postBob1);

        // David's post (friend-of-friend-of-friend - distance 3)
        Vertex postDavid = builder.createPost(POST_DAVID_ID, "David post March", DATE_MAR_15, newYork, david);
        techTalk.addEdge(CONTAINER_OF, postDavid);
    }

    @Test
    void testFindMessagesFromDirectFriend() {
        ComplexReadQuery9 query = new ComplexReadQuery9();
        // Get messages before March
        LdbcQuery9 operation = new LdbcQuery9(ALICE_ID, DATE_MAR_01, 20);

        List<LdbcQuery9Result> results = executeQuery(query, operation);

        // Should find Bob's messages (direct friend)
        boolean foundBobPost1 = results.stream()
                .anyMatch(r -> r.getMessageId() == POST_BOB_1_ID);
        boolean foundBobPost2 = results.stream()
                .anyMatch(r -> r.getMessageId() == POST_BOB_2_ID);

        assertTrue(foundBobPost1, "Should find Bob's January post");
        assertTrue(foundBobPost2, "Should find Bob's February post");
    }

    @Test
    void testFindMessagesFromFriendOfFriend() {
        ComplexReadQuery9 query = new ComplexReadQuery9();
        LdbcQuery9 operation = new LdbcQuery9(ALICE_ID, DATE_MAR_01, 20);

        List<LdbcQuery9Result> results = executeQuery(query, operation);

        // Should find Carol's comment (friend-of-friend)
        boolean foundCarolComment = results.stream()
                .anyMatch(r -> r.getMessageId() == COMMENT_CAROL_ID);

        assertTrue(foundCarolComment, "Should find Carol's comment (friend-of-friend)");
    }

    @Test
    void testFindMessagesFromDistance3Friend() {
        ComplexReadQuery9 query = new ComplexReadQuery9();
        // Include David's post (after March 15)
        LdbcQuery9 operation = new LdbcQuery9(ALICE_ID, new Date(DATE_MAR_15.getTime() + 86400000), 20);

        List<LdbcQuery9Result> results = executeQuery(query, operation);

        // David is at distance 3 (Alice -> Bob -> Carol -> David)
        // IC9 should include friends AND friends-of-friends (distance 1 and 2)
        boolean foundDavidPost = results.stream()
                .anyMatch(r -> r.getMessageId() == POST_DAVID_ID);

        // Note: LDBC spec says "friends or friends of friends" which means distance 1-2
        // David at distance 3 should NOT be included
        assertFalse(foundDavidPost, "David at distance 3 should NOT be included");
    }

    @Test
    void testMaxDateExclusion() {
        ComplexReadQuery9 query = new ComplexReadQuery9();
        // maxDate = Feb 1, should exclude Feb 15 post
        LdbcQuery9 operation = new LdbcQuery9(ALICE_ID, DATE_FEB_01, 20);

        List<LdbcQuery9Result> results = executeQuery(query, operation);

        // Should NOT find Feb 15 post (after maxDate)
        boolean foundFebPost = results.stream()
                .anyMatch(r -> r.getMessageId() == POST_BOB_2_ID);

        assertFalse(foundFebPost, "Should not find posts on or after maxDate");

        // Should find January post
        boolean foundJanPost = results.stream()
                .anyMatch(r -> r.getMessageId() == POST_BOB_1_ID);

        assertTrue(foundJanPost, "Should find posts before maxDate");
    }

    @Test
    void testResultsOrderedByDateDescThenIdAsc() {
        ComplexReadQuery9 query = new ComplexReadQuery9();
        LdbcQuery9 operation = new LdbcQuery9(ALICE_ID, DATE_MAR_01, 20);

        List<LdbcQuery9Result> results = executeQuery(query, operation);

        // Verify ordering
        for (int i = 0; i < results.size() - 1; i++) {
            LdbcQuery9Result current = results.get(i);
            LdbcQuery9Result next = results.get(i + 1);

            if (current.getMessageCreationDate() == next.getMessageCreationDate()) {
                // Same date - should be ordered by ID ASC
                assertTrue(current.getMessageId() < next.getMessageId(),
                        "Same date results should be ordered by ID ASC");
            } else {
                // Different dates - should be ordered by date DESC
                assertTrue(current.getMessageCreationDate() > next.getMessageCreationDate(),
                        "Results should be ordered by creationDate DESC");
            }
        }
    }

    @Test
    void testLimitResults() {
        ComplexReadQuery9 query = new ComplexReadQuery9();
        LdbcQuery9 operation = new LdbcQuery9(ALICE_ID, DATE_MAR_01, 2);

        List<LdbcQuery9Result> results = executeQuery(query, operation);

        assertEquals(2, results.size(), "Should be limited to 2 results");
    }

    @Test
    void testResultContainsCorrectFields() {
        ComplexReadQuery9 query = new ComplexReadQuery9();
        LdbcQuery9 operation = new LdbcQuery9(ALICE_ID, DATE_MAR_01, 20);

        List<LdbcQuery9Result> results = executeQuery(query, operation);

        // Find Bob's post and verify all fields
        LdbcQuery9Result bobPost = results.stream()
                .filter(r -> r.getMessageId() == POST_BOB_1_ID)
                .findFirst()
                .orElse(null);

        assertNotNull(bobPost, "Should find Bob's post");
        assertEquals(BOB_ID, bobPost.getPersonId());
        assertEquals("Bob", bobPost.getPersonFirstName());
        assertEquals("Brown", bobPost.getPersonLastName());
        assertEquals("Bob post January", bobPost.getMessageContent());
        assertEquals(DATE_JAN_15.getTime(), bobPost.getMessageCreationDate());
    }

    @Test
    void testStartPersonMessagesExcluded() {
        ComplexReadQuery9 query = new ComplexReadQuery9();
        // Alice has posts in the fixture (POST1_ID, POST2_ID)
        LdbcQuery9 operation = new LdbcQuery9(ALICE_ID, DATE_MAR_01, 20);

        List<LdbcQuery9Result> results = executeQuery(query, operation);

        // Alice's own messages should not be in results
        boolean foundAlicePost = results.stream()
                .anyMatch(r -> r.getPersonId() == ALICE_ID);

        assertFalse(foundAlicePost, "Start person's messages should be excluded");
    }
}
