package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery8;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery8Result;

import java.util.Date;
import java.util.List;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * IC8: Recent replies
 *
 * Given a start Person, find the most recent Comments that are replies to Messages of the
 * start Person. Only consider direct (single-hop) replies, not the transitive (multi-hop)
 * ones. Return the reply Comments, and the Person that created each reply Comment.
 */
class ComplexQuery8Test extends AbstractQueryTest {

    private static final long COMMENT_CAROL_ID = 460L;
    private static final long COMMENT_DAVID_ID = 461L;
    private static final long COMMENT_BOB_REPLY_ID = 462L;

    private static final Date DATE_FEB = new Date(1643702400000L); // 2022-02-01
    private static final Date DATE_MAR = new Date(1646121600000L); // 2022-03-01
    private static final Date DATE_APR = new Date(1648800000000L); // 2022-04-01

    @BeforeEach
    void setUpAdditionalData() {
        TestDataBuilder builder = builder();

        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();
        Vertex carol = g.V().has(PERSON, ID, CAROL_ID).next();
        Vertex david = g.V().has(PERSON, ID, DAVID_ID).next();
        Vertex post1 = g.V().has(POST, ID, POST1_ID).next();
        Vertex post2 = g.V().has(POST, ID, POST2_ID).next();
        Vertex newYork = g.V().has(PLACE, ID, NEW_YORK_ID).next();

        // Carol's comment replying to Alice's post1
        Vertex commentCarol = builder.createComment(COMMENT_CAROL_ID, "Carol's reply", DATE_FEB, newYork, carol);
        commentCarol.addEdge(REPLY_OF, post1);

        // David's comment replying to Alice's post2
        Vertex commentDavid = builder.createComment(COMMENT_DAVID_ID, "David's reply", DATE_MAR, newYork, david);
        commentDavid.addEdge(REPLY_OF, post2);

        // Bob's reply to Alice's post1 (another one, more recent)
        Vertex commentBobReply = builder.createComment(COMMENT_BOB_REPLY_ID, "Bob's second reply", DATE_APR, newYork, bob);
        commentBobReply.addEdge(REPLY_OF, post1);

        // Reply to Bob's comment (NOT a direct reply to Alice's message - should be excluded)
        Vertex comment1 = g.V().has(COMMENT, ID, COMMENT1_ID).next();
        Vertex nestedReply = builder.createComment(463L, "Nested reply", DATE_APR, newYork, carol);
        nestedReply.addEdge(REPLY_OF, comment1); // Reply to comment, not to Alice's post
    }

    @Test
    void testFindDirectReplies() {
        ComplexReadQuery8 query = new ComplexReadQuery8();
        LdbcQuery8 operation = new LdbcQuery8(ALICE_ID, 10);

        List<LdbcQuery8Result> results = executeQuery(query, operation);

        // Should find Bob's comments (from fixture and new), Carol's, David's
        assertTrue(results.size() >= 3, "Should find at least 3 direct replies");

        boolean foundCarol = results.stream().anyMatch(r -> r.getPersonId() == CAROL_ID);
        boolean foundDavid = results.stream().anyMatch(r -> r.getPersonId() == DAVID_ID);
        boolean foundBob = results.stream().anyMatch(r -> r.getPersonId() == BOB_ID);

        assertTrue(foundCarol, "Carol's reply should be found");
        assertTrue(foundDavid, "David's reply should be found");
        assertTrue(foundBob, "Bob's reply should be found");
    }

    @Test
    void testOrderingByDateDescThenIdAsc() {
        ComplexReadQuery8 query = new ComplexReadQuery8();
        LdbcQuery8 operation = new LdbcQuery8(ALICE_ID, 10);

        List<LdbcQuery8Result> results = executeQuery(query, operation);

        // Verify ordering
        for (int i = 0; i < results.size() - 1; i++) {
            LdbcQuery8Result current = results.get(i);
            LdbcQuery8Result next = results.get(i + 1);

            if (current.getCommentCreationDate() == next.getCommentCreationDate()) {
                assertTrue(current.getCommentId() < next.getCommentId(),
                        "Same date should be ordered by comment ID ASC");
            } else {
                assertTrue(current.getCommentCreationDate() > next.getCommentCreationDate(),
                        "Results should be ordered by date DESC");
            }
        }
    }

    @Test
    void testResultContainsCorrectFields() {
        ComplexReadQuery8 query = new ComplexReadQuery8();
        LdbcQuery8 operation = new LdbcQuery8(ALICE_ID, 10);

        List<LdbcQuery8Result> results = executeQuery(query, operation);

        // Find Carol's reply
        LdbcQuery8Result carolResult = results.stream()
                .filter(r -> r.getCommentId() == COMMENT_CAROL_ID)
                .findFirst()
                .orElse(null);

        assertNotNull(carolResult, "Should find Carol's comment");
        assertEquals(CAROL_ID, carolResult.getPersonId());
        assertEquals("Carol", carolResult.getPersonFirstName());
        assertEquals("Clark", carolResult.getPersonLastName());
        assertEquals("Carol's reply", carolResult.getCommentContent());
        assertEquals(DATE_FEB.getTime(), carolResult.getCommentCreationDate());
    }

    @Test
    void testLimitResults() {
        ComplexReadQuery8 query = new ComplexReadQuery8();
        LdbcQuery8 operation = new LdbcQuery8(ALICE_ID, 2);

        List<LdbcQuery8Result> results = executeQuery(query, operation);

        assertEquals(2, results.size(), "Should be limited to 2 results");
    }

    @Test
    void testOnlyDirectRepliesNotNested() {
        ComplexReadQuery8 query = new ComplexReadQuery8();
        LdbcQuery8 operation = new LdbcQuery8(ALICE_ID, 20);

        List<LdbcQuery8Result> results = executeQuery(query, operation);

        // The nested reply (463) should NOT be included
        boolean foundNestedReply = results.stream().anyMatch(r -> r.getCommentId() == 463L);
        assertFalse(foundNestedReply, "Nested reply (reply to comment) should NOT be included");
    }

    @Test
    void testMostRecentFirst() {
        ComplexReadQuery8 query = new ComplexReadQuery8();
        LdbcQuery8 operation = new LdbcQuery8(ALICE_ID, 10);

        List<LdbcQuery8Result> results = executeQuery(query, operation);

        // Most recent should be first (April comment)
        assertFalse(results.isEmpty());
        assertEquals(COMMENT_BOB_REPLY_ID, results.get(0).getCommentId(),
                "Most recent reply (April) should be first");
    }

}
