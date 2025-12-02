package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery2;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery2Result;

import java.util.Date;
import java.util.List;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * IC2: Recent messages from friends
 *
 * Given a start Person, find the most recent Messages created by that Person's
 * friends (including the date they were created). Only consider Messages created
 * before a given maxDate.
 *
 * Test graph adds messages from friends with various dates.
 */
class ComplexQuery2Test extends AbstractQueryTest {

    private static final long POST_BOB_1_ID = 310L;
    private static final long POST_BOB_2_ID = 311L;
    private static final long COMMENT_BOB_1_ID = 410L;
    private static final long POST_CAROL_1_ID = 320L;

    // Dates for ordering tests
    private static final Date DATE_FEB_2022 = new Date(1643702400000L); // 2022-02-01
    private static final Date DATE_MAR_2022 = new Date(1646121600000L); // 2022-03-01
    private static final Date DATE_APR_2022 = new Date(1648800000000L); // 2022-04-01
    private static final Date DATE_MAY_2022 = new Date(1651392000000L); // 2022-05-01

    @BeforeEach
    void setUpAdditionalData() {
        TestDataBuilder builder = builder();

        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();
        Vertex carol = g.V().has(PERSON, ID, CAROL_ID).next();
        Vertex newYork = g.V().has(PLACE, ID, NEW_YORK_ID).next();
        Vertex techTalk = g.V().has(FORUM, ID, TECH_TALK_ID).next();

        // Bob's posts (friend of Alice)
        Vertex postBob1 = builder.createPost(POST_BOB_1_ID, "Bob's first post", DATE_FEB_2022, newYork, bob);
        techTalk.addEdge(CONTAINER_OF, postBob1);

        Vertex postBob2 = builder.createPost(POST_BOB_2_ID, "Bob's second post", DATE_APR_2022, newYork, bob);
        techTalk.addEdge(CONTAINER_OF, postBob2);

        // Bob's comment
        Vertex post1 = g.V().has(POST, ID, POST1_ID).next();
        Vertex commentBob1 = builder.createComment(COMMENT_BOB_1_ID, "Bob's comment", DATE_MAR_2022, newYork, bob);
        commentBob1.addEdge(REPLY_OF, post1);

        // Carol's post (friend-of-friend of Alice via Bob)
        Vertex postCarol1 = builder.createPost(POST_CAROL_1_ID, "Carol's post", DATE_MAY_2022, newYork, carol);
        techTalk.addEdge(CONTAINER_OF, postCarol1);
    }

    @Test
    void testRecentMessagesFromFriend() {
        ComplexReadQuery2 query = new ComplexReadQuery2();
        // Get Bob's messages before May 2022
        LdbcQuery2 operation = new LdbcQuery2(ALICE_ID, DATE_MAY_2022, 10);

        List<LdbcQuery2Result> results = executeQuery(query, operation);

        // Bob has: post (Feb), comment (Mar), post (Apr) - all before May
        // Carol is friend-of-friend so included: post (May) - but it's not before May
        // Plus Bob's comment from fixture (DATE_2022 = Jan 1 2022)
        assertTrue(results.size() >= 3, "Should find at least Bob's 3 messages");

        // Verify ordering by creationDate DESC
        for (int i = 0; i < results.size() - 1; i++) {
            assertTrue(results.get(i).getMessageCreationDate() >= results.get(i + 1).getMessageCreationDate(),
                    "Results should be ordered by creationDate DESC");
        }
    }

    @Test
    void testMessagesOrderedByDateDesc() {
        ComplexReadQuery2 query = new ComplexReadQuery2();
        // Get all messages before June (includes everything)
        LdbcQuery2 operation = new LdbcQuery2(ALICE_ID, new Date(1654041600000L), 10); // 2022-06-01

        List<LdbcQuery2Result> results = executeQuery(query, operation);

        // Most recent should be first
        assertTrue(results.size() >= 3);

        // First result should be the most recent
        long prevDate = Long.MAX_VALUE;
        for (LdbcQuery2Result result : results) {
            assertTrue(result.getMessageCreationDate() <= prevDate,
                    "Messages should be in descending date order");
            prevDate = result.getMessageCreationDate();
        }
    }

    @Test
    void testMaxDateFilter() {
        ComplexReadQuery2 query = new ComplexReadQuery2();
        // Get messages before March 2022 - should exclude Mar, Apr, May messages
        LdbcQuery2 operation = new LdbcQuery2(ALICE_ID, DATE_MAR_2022, 10);

        List<LdbcQuery2Result> results = executeQuery(query, operation);

        // All results should be before March 2022
        for (LdbcQuery2Result result : results) {
            assertTrue(result.getMessageCreationDate() < DATE_MAR_2022.getTime(),
                    "All messages should be before maxDate");
        }
    }

    @Test
    void testLimitResults() {
        ComplexReadQuery2 query = new ComplexReadQuery2();
        LdbcQuery2 operation = new LdbcQuery2(ALICE_ID, new Date(1654041600000L), 2); // Limit to 2

        List<LdbcQuery2Result> results = executeQuery(query, operation);

        assertEquals(2, results.size(), "Should be limited to 2 results");
    }

    @Test
    void testResultContainsCorrectFields() {
        ComplexReadQuery2 query = new ComplexReadQuery2();
        LdbcQuery2 operation = new LdbcQuery2(ALICE_ID, DATE_MAY_2022, 10);

        List<LdbcQuery2Result> results = executeQuery(query, operation);

        assertFalse(results.isEmpty());

        // Find Bob's post for verification
        LdbcQuery2Result bobsPost = results.stream()
                .filter(r -> r.getMessageId() == POST_BOB_2_ID)
                .findFirst()
                .orElse(null);

        if (bobsPost != null) {
            assertEquals(BOB_ID, bobsPost.getPersonId());
            assertEquals("Bob", bobsPost.getPersonFirstName());
            assertEquals("Brown", bobsPost.getPersonLastName());
            assertEquals("Bob's second post", bobsPost.getMessageContent());
        }
    }

    @Test
    void testOnlyDirectFriendsIncluded() {
        ComplexReadQuery2 query = new ComplexReadQuery2();
        // Get messages including Carol's post in May
        LdbcQuery2 operation = new LdbcQuery2(ALICE_ID, new Date(1654041600000L), 20); // June 2022

        List<LdbcQuery2Result> results = executeQuery(query, operation);

        // IC2 only includes direct friends (not friend-of-friends)
        // Carol is friend-of-friend (Alice -> Bob -> Carol), so should NOT be included
        boolean foundCarolsPost = results.stream()
                .anyMatch(r -> r.getPersonId() == CAROL_ID);

        assertFalse(foundCarolsPost, "Friend-of-friend messages should NOT be included in IC2");

        // But Bob's messages should be included (direct friend)
        boolean foundBobsPost = results.stream()
                .anyMatch(r -> r.getPersonId() == BOB_ID);

        assertTrue(foundBobsPost, "Direct friend's messages should be included");
    }
}
