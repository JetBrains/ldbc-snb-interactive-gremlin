package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery4;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery4Result;

import java.util.Date;
import java.util.List;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * IC4: New topics
 *
 * Given a start Person, find Tags that are attached to Posts that were created by that
 * Person's friends. Only include Tags that were attached to friends' Posts created within
 * a given time interval and that were never attached to friends' Posts created before
 * this interval.
 */
class ComplexQuery4Test extends AbstractQueryTest {

    private static final long TAG_PYTHON_ID = 510L;
    private static final long TAG_RUST_ID = 511L;
    private static final long TAG_GO_ID = 512L;

    // Date window: 2022-01-01 to 2022-12-31
    private static final Date START_DATE = new Date(1640995200000L); // 2022-01-01
    private static final int DURATION_DAYS = 365;

    @BeforeEach
    void setUpAdditionalData() {
        TestDataBuilder builder = builder();

        // Create new tags
        Vertex tagPython = builder.createTag(TAG_PYTHON_ID, "Python");
        Vertex tagRust = builder.createTag(TAG_RUST_ID, "Rust");
        Vertex tagGo = builder.createTag(TAG_GO_ID, "Go");

        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();
        Vertex newYork = g.V().has(PLACE, ID, NEW_YORK_ID).next();
        Vertex tagJava = g.V().has(TAG, ID, TAG_JAVA_ID).next();

        // Bob's OLD post (before window) with Java tag - Java should be excluded as "prior tag"
        Vertex oldPost = builder.createPost(360L, "Old Java post", new Date(1609459200000L), newYork, bob);
        oldPost.addEdge(HAS_TAG, tagJava);

        // Bob's NEW posts (within window) with new tags
        Vertex post1 = builder.createPost(361L, "Python is great", new Date(1643702400000L), newYork, bob);
        post1.addEdge(HAS_TAG, tagPython);

        Vertex post2 = builder.createPost(362L, "More Python", new Date(1646121600000L), newYork, bob);
        post2.addEdge(HAS_TAG, tagPython);

        Vertex post3 = builder.createPost(363L, "Learning Rust", new Date(1648800000000L), newYork, bob);
        post3.addEdge(HAS_TAG, tagRust);

        // Post with Java tag in window - should be excluded since Java was used before
        Vertex post4 = builder.createPost(364L, "Java again", new Date(1651392000000L), newYork, bob);
        post4.addEdge(HAS_TAG, tagJava);
    }

    @Test
    void testFindNewTags() {
        ComplexReadQuery4 query = new ComplexReadQuery4();
        LdbcQuery4 operation = new LdbcQuery4(ALICE_ID, START_DATE, DURATION_DAYS, 10);

        List<LdbcQuery4Result> results = executeQuery(query, operation);

        // Should find Python and Rust (new tags in window)
        // Should NOT find Java (used before window)
        assertFalse(results.isEmpty(), "Should find new tags");

        boolean foundPython = results.stream().anyMatch(r -> r.getTagName().equals("Python"));
        boolean foundRust = results.stream().anyMatch(r -> r.getTagName().equals("Rust"));
        boolean foundJava = results.stream().anyMatch(r -> r.getTagName().equals("Java"));

        assertTrue(foundPython, "Python should be found (new tag)");
        assertTrue(foundRust, "Rust should be found (new tag)");
        assertFalse(foundJava, "Java should NOT be found (used before window)");
    }

    @Test
    void testTagCountsAreCorrect() {
        ComplexReadQuery4 query = new ComplexReadQuery4();
        LdbcQuery4 operation = new LdbcQuery4(ALICE_ID, START_DATE, DURATION_DAYS, 10);

        List<LdbcQuery4Result> results = executeQuery(query, operation);

        // Python has 2 posts, Rust has 1 post
        LdbcQuery4Result pythonResult = results.stream()
                .filter(r -> r.getTagName().equals("Python"))
                .findFirst()
                .orElse(null);

        assertNotNull(pythonResult, "Should find Python tag");
        assertEquals(2, pythonResult.getPostCount(), "Python should have 2 posts");

        LdbcQuery4Result rustResult = results.stream()
                .filter(r -> r.getTagName().equals("Rust"))
                .findFirst()
                .orElse(null);

        assertNotNull(rustResult, "Should find Rust tag");
        assertEquals(1, rustResult.getPostCount(), "Rust should have 1 post");
    }

    @Test
    void testOrderingByCountDescThenNameAsc() {
        ComplexReadQuery4 query = new ComplexReadQuery4();
        LdbcQuery4 operation = new LdbcQuery4(ALICE_ID, START_DATE, DURATION_DAYS, 10);

        List<LdbcQuery4Result> results = executeQuery(query, operation);

        // Verify ordering
        for (int i = 0; i < results.size() - 1; i++) {
            LdbcQuery4Result current = results.get(i);
            LdbcQuery4Result next = results.get(i + 1);

            if (current.getPostCount() == next.getPostCount()) {
                assertTrue(current.getTagName().compareTo(next.getTagName()) < 0,
                        "Same count should be ordered by tag name ASC");
            } else {
                assertTrue(current.getPostCount() > next.getPostCount(),
                        "Results should be ordered by count DESC");
            }
        }
    }

    @Test
    void testLimitResults() {
        ComplexReadQuery4 query = new ComplexReadQuery4();
        LdbcQuery4 operation = new LdbcQuery4(ALICE_ID, START_DATE, DURATION_DAYS, 1);

        List<LdbcQuery4Result> results = executeQuery(query, operation);

        assertEquals(1, results.size(), "Should be limited to 1 result");
    }

    @Test
    void testEmptyResultWhenNoNewTags() {
        ComplexReadQuery4 query = new ComplexReadQuery4();
        // Use a date range before any posts
        LdbcQuery4 operation = new LdbcQuery4(ALICE_ID, new Date(1577836800000L), 30, 10); // 2020-01-01 for 30 days

        List<LdbcQuery4Result> results = executeQuery(query, operation);

        // No posts from friends in this window
        assertTrue(results.isEmpty(), "Should have no results when no new tags");
    }
}
