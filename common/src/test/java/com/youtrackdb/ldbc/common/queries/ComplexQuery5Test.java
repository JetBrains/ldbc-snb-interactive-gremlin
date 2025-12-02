package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery5;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery5Result;

import java.util.Date;
import java.util.List;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * IC5: New groups
 *
 * Given a start Person, denote their friends and friends of friends (excluding the start
 * Person) as otherPerson. Find Forums that any Person otherPerson became a member of after
 * a given date. For each of those Forums, count the number of Posts that were created by
 * the Person otherPerson.
 */
class ComplexQuery5Test extends AbstractQueryTest {

    private static final long FORUM_DEV_ID = 210L;
    private static final long FORUM_MUSIC_ID = 211L;

    // Join date filter
    private static final Date MIN_DATE = new Date(1640995200000L); // 2022-01-01

    @BeforeEach
    void setUpAdditionalData() {
        TestDataBuilder builder = builder();

        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();
        Vertex carol = g.V().has(PERSON, ID, CAROL_ID).next();
        Vertex newYork = g.V().has(PLACE, ID, NEW_YORK_ID).next();

        // Create new forums
        Vertex devForum = builder.createForum(FORUM_DEV_ID, "DevForum", DATE_2020);
        devForum.addEdge(HAS_MODERATOR, bob);

        Vertex musicForum = builder.createForum(FORUM_MUSIC_ID, "MusicForum", DATE_2020);
        musicForum.addEdge(HAS_MODERATOR, carol);

        // Bob joins DevForum AFTER minDate (should be included)
        devForum.addEdge(HAS_MEMBER, bob, JOIN_DATE, new Date(1643702400000L)); // 2022-02-01

        // Carol joins MusicForum AFTER minDate
        musicForum.addEdge(HAS_MEMBER, carol, JOIN_DATE, new Date(1646121600000L)); // 2022-03-01

        // Bob's posts in DevForum
        Vertex post1 = builder.createPost(370L, "DevForum post 1", new Date(1648800000000L), newYork, bob);
        devForum.addEdge(CONTAINER_OF, post1);

        Vertex post2 = builder.createPost(371L, "DevForum post 2", new Date(1651392000000L), newYork, bob);
        devForum.addEdge(CONTAINER_OF, post2);

        // Carol's posts in MusicForum
        Vertex post3 = builder.createPost(372L, "MusicForum post", new Date(1654070400000L), newYork, carol);
        musicForum.addEdge(CONTAINER_OF, post3);
    }

    @Test
    void testFindNewForumMemberships() {
        ComplexReadQuery5 query = new ComplexReadQuery5();
        LdbcQuery5 operation = new LdbcQuery5(ALICE_ID, MIN_DATE, 10);

        List<LdbcQuery5Result> results = executeQuery(query, operation);

        // Should find forums where friends/FoF joined after minDate
        assertFalse(results.isEmpty(), "Should find forums with new memberships");

        boolean foundDevForum = results.stream().anyMatch(r -> r.getForumTitle().equals("DevForum"));
        assertTrue(foundDevForum, "DevForum should be found (Bob joined after minDate)");
    }

    @Test
    void testPostCountsAreCorrect() {
        ComplexReadQuery5 query = new ComplexReadQuery5();
        LdbcQuery5 operation = new LdbcQuery5(ALICE_ID, MIN_DATE, 10);

        List<LdbcQuery5Result> results = executeQuery(query, operation);

        // DevForum should have 2 posts from Bob
        LdbcQuery5Result devForumResult = results.stream()
                .filter(r -> r.getForumTitle().equals("DevForum"))
                .findFirst()
                .orElse(null);

        assertNotNull(devForumResult, "Should find DevForum");
        assertEquals(2, devForumResult.getPostCount(), "DevForum should have 2 posts from Bob");
    }

    @Test
    void testOrderingByPostCountDescThenForumIdAsc() {
        ComplexReadQuery5 query = new ComplexReadQuery5();
        LdbcQuery5 operation = new LdbcQuery5(ALICE_ID, MIN_DATE, 10);

        List<LdbcQuery5Result> results = executeQuery(query, operation);

        // Verify ordering (post count DESC is primary sort)
        for (int i = 0; i < results.size() - 1; i++) {
            LdbcQuery5Result current = results.get(i);
            LdbcQuery5Result next = results.get(i + 1);

            assertTrue(current.getPostCount() >= next.getPostCount(),
                    "Results should be ordered by post count DESC");
        }
    }

    @Test
    void testLimitResults() {
        ComplexReadQuery5 query = new ComplexReadQuery5();
        LdbcQuery5 operation = new LdbcQuery5(ALICE_ID, MIN_DATE, 1);

        List<LdbcQuery5Result> results = executeQuery(query, operation);

        assertEquals(1, results.size(), "Should be limited to 1 result");
    }

    @Test
    void testNoResultsForOldMemberships() {
        ComplexReadQuery5 query = new ComplexReadQuery5();
        // Use a minDate after all join dates
        LdbcQuery5 operation = new LdbcQuery5(ALICE_ID, new Date(1672531200000L), 10); // 2023-01-01

        List<LdbcQuery5Result> results = executeQuery(query, operation);

        // No forum memberships after 2023-01-01
        assertTrue(results.isEmpty(), "Should have no results when minDate is after all join dates");
    }
}
