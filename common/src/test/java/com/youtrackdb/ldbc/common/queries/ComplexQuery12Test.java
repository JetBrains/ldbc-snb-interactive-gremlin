package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery12;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery12Result;

import java.util.ArrayList;
import java.util.List;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * IC12: Expert search
 *
 * Given a start Person, find the Comments that this Person's friends made in reply to Posts,
 * considering only those Comments that are direct (single-hop) replies to Posts. Only consider
 * Posts with a Tag in a given TagClass or in a descendent of that TagClass. Count the number
 * of these reply Comments, and collect the Tags.
 */
class ComplexQuery12Test extends AbstractQueryTest {

    private static final long TAG_CLASS_PROG_ID = 700L;
    private static final long TAG_CLASS_DB_ID = 701L;
    private static final long TAG_PYTHON_ID = 510L;

    @BeforeEach
    void setUpAdditionalData() {
        TestDataBuilder builder = builder();

        // Create TagClasses
        Vertex tagClassProgramming = g.addV(TAG_CLASS)
                .property(ID, TAG_CLASS_PROG_ID)
                .property(NAME, "Programming")
                .property(URL, "http://example.com/tagclass/programming")
                .next();

        Vertex tagClassDatabase = g.addV(TAG_CLASS)
                .property(ID, TAG_CLASS_DB_ID)
                .property(NAME, "Database")
                .property(URL, "http://example.com/tagclass/database")
                .next();

        // Database is subclass of Programming
        tagClassDatabase.addEdge(IS_SUBCLASS_OF, tagClassProgramming);

        // Link existing tags to TagClasses
        Vertex tagJava = g.V().has(TAG, ID, TAG_JAVA_ID).next();
        Vertex tagDb = g.V().has(TAG, ID, TAG_DB_ID).next();
        tagJava.addEdge(HAS_TYPE, tagClassProgramming);
        tagDb.addEdge(HAS_TYPE, tagClassDatabase);

        // Create Python tag in Programming class
        Vertex tagPython = builder.createTag(TAG_PYTHON_ID, "Python");
        tagPython.addEdge(HAS_TYPE, tagClassProgramming);

        // Create posts with tags in the Programming class hierarchy
        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();
        Vertex newYork = g.V().has(PLACE, ID, NEW_YORK_ID).next();
        Vertex alice = g.V().has(PERSON, ID, ALICE_ID).next();

        // Alice's post with Java tag (in Programming class)
        Vertex aliceJavaPost = builder.createPost(395L, "Alice Java question", DATE_2022, newYork, alice);
        aliceJavaPost.addEdge(HAS_TAG, tagJava);

        // Alice's post with Databases tag (in Database class, which is subclass of Programming)
        Vertex aliceDbPost = builder.createPost(396L, "Alice DB question", DATE_2022, newYork, alice);
        aliceDbPost.addEdge(HAS_TAG, tagDb);

        // Bob replies to Alice's Java post (Bob is direct friend)
        Vertex bobReply1 = builder.createComment(470L, "Bob's Java reply", DATE_2022, newYork, bob);
        bobReply1.addEdge(REPLY_OF, aliceJavaPost);

        Vertex bobReply2 = builder.createComment(471L, "Bob's second Java reply", DATE_2022, newYork, bob);
        bobReply2.addEdge(REPLY_OF, aliceJavaPost);

        // Bob replies to Alice's DB post
        Vertex bobDbReply = builder.createComment(472L, "Bob's DB reply", DATE_2022, newYork, bob);
        bobDbReply.addEdge(REPLY_OF, aliceDbPost);
    }

    @Test
    void testFindFriendsWithRepliesInTagClass() {
        ComplexReadQuery12 query = new ComplexReadQuery12();
        LdbcQuery12 operation = new LdbcQuery12(ALICE_ID, "Programming", 10);

        List<LdbcQuery12Result> results = executeQuery(query, operation);

        // Should find Bob (replied to Java and DB posts, both in Programming hierarchy)
        assertFalse(results.isEmpty(), "Should find friends with replies");

        boolean foundBob = results.stream().anyMatch(r -> r.getPersonId() == BOB_ID);
        assertTrue(foundBob, "Bob should be found (replied to Programming-related posts)");
    }

    @Test
    void testIncludesSubclassTags() {
        ComplexReadQuery12 query = new ComplexReadQuery12();
        // Search for Programming class - should include Database (subclass)
        LdbcQuery12 operation = new LdbcQuery12(ALICE_ID, "Programming", 10);

        List<LdbcQuery12Result> results = executeQuery(query, operation);

        LdbcQuery12Result bobResult = results.stream()
                .filter(r -> r.getPersonId() == BOB_ID)
                .findFirst()
                .orElse(null);

        assertNotNull(bobResult, "Should find Bob");

        // Bob replied to posts with Java (Programming) and Databases (Database->Programming)
        // replyCount should include replies to both
        assertTrue(bobResult.getReplyCount() >= 3,
                "Should count replies to posts in Programming and its subclasses");
    }

    @Test
    void testTagNamesCollected() {
        ComplexReadQuery12 query = new ComplexReadQuery12();
        LdbcQuery12 operation = new LdbcQuery12(ALICE_ID, "Programming", 10);

        List<LdbcQuery12Result> results = executeQuery(query, operation);

        LdbcQuery12Result bobResult = results.stream()
                .filter(r -> r.getPersonId() == BOB_ID)
                .findFirst()
                .orElse(null);

        assertNotNull(bobResult, "Should find Bob");
        assertNotNull(bobResult.getTagNames(), "Tag names should not be null");
        List<String> tagNames = toList(bobResult.getTagNames());
        assertFalse(tagNames.isEmpty(), "Tag names should not be empty");

        // Tags should include Java and/or Databases
        assertTrue(tagNames.contains("Java") || tagNames.contains("Databases"),
                "Should collect relevant tag names");
    }

    @Test
    void testOrderingByReplyCountDescThenIdAsc() {
        ComplexReadQuery12 query = new ComplexReadQuery12();
        LdbcQuery12 operation = new LdbcQuery12(ALICE_ID, "Programming", 10);

        List<LdbcQuery12Result> results = executeQuery(query, operation);

        // Verify ordering
        for (int i = 0; i < results.size() - 1; i++) {
            LdbcQuery12Result current = results.get(i);
            LdbcQuery12Result next = results.get(i + 1);

            if (current.getReplyCount() == next.getReplyCount()) {
                assertTrue(current.getPersonId() < next.getPersonId(),
                        "Same reply count should be ordered by personId ASC");
            } else {
                assertTrue(current.getReplyCount() > next.getReplyCount(),
                        "Results should be ordered by reply count DESC");
            }
        }
    }

    @Test
    void testResultContainsCorrectFields() {
        ComplexReadQuery12 query = new ComplexReadQuery12();
        LdbcQuery12 operation = new LdbcQuery12(ALICE_ID, "Programming", 10);

        List<LdbcQuery12Result> results = executeQuery(query, operation);

        LdbcQuery12Result bobResult = results.stream()
                .filter(r -> r.getPersonId() == BOB_ID)
                .findFirst()
                .orElse(null);

        assertNotNull(bobResult, "Should find Bob");
        assertEquals("Bob", bobResult.getPersonFirstName());
        assertEquals("Brown", bobResult.getPersonLastName());
        assertTrue(bobResult.getReplyCount() > 0, "Reply count should be positive");
    }

    @Test
    void testLimitResults() {
        ComplexReadQuery12 query = new ComplexReadQuery12();
        LdbcQuery12 operation = new LdbcQuery12(ALICE_ID, "Programming", 1);

        List<LdbcQuery12Result> results = executeQuery(query, operation);

        assertTrue(results.size() <= 1, "Should be limited to 1 result");
    }

    @Test
    void testNoResultsForNonMatchingTagClass() {
        ComplexReadQuery12 query = new ComplexReadQuery12();
        LdbcQuery12 operation = new LdbcQuery12(ALICE_ID, "NonExistentTagClass", 10);

        List<LdbcQuery12Result> results = executeQuery(query, operation);

        assertTrue(results.isEmpty(), "Should have no results for non-matching tag class");
    }

    private <T> List<T> toList(Iterable<T> iterable) {
        List<T> list = new ArrayList<>();
        iterable.forEach(list::add);
        return list;
    }
}
