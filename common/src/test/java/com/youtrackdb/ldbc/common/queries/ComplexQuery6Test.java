package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery6;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery6Result;

import java.util.List;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * IC6: Tag co-occurrence
 *
 * Given a start Person and a Tag, find the other Tags that occur together with this Tag on
 * Posts that were created by start Person's friends and friends of friends (excluding
 * start Person). Return top 10 Tags, and the count of Posts that contain both this Tag and
 * the given Tag.
 */
class ComplexQuery6Test extends AbstractQueryTest {

    private static final long TAG_PYTHON_ID = 510L;
    private static final long TAG_RUST_ID = 511L;
    private static final long TAG_GO_ID = 512L;

    @BeforeEach
    void setUpAdditionalData() {
        TestDataBuilder builder = builder();

        // Create new tags
        Vertex tagPython = builder.createTag(TAG_PYTHON_ID, "Python");
        Vertex tagRust = builder.createTag(TAG_RUST_ID, "Rust");
        Vertex tagGo = builder.createTag(TAG_GO_ID, "Go");

        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();
        Vertex carol = g.V().has(PERSON, ID, CAROL_ID).next();
        Vertex newYork = g.V().has(PLACE, ID, NEW_YORK_ID).next();
        Vertex tagJava = g.V().has(TAG, ID, TAG_JAVA_ID).next();

        // Bob's posts with Java + other tags (co-occurrence)
        Vertex post1 = builder.createPost(380L, "Java and Python", DATE_2022, newYork, bob);
        post1.addEdge(HAS_TAG, tagJava);
        post1.addEdge(HAS_TAG, tagPython);

        Vertex post2 = builder.createPost(381L, "Java and Python again", DATE_2022, newYork, bob);
        post2.addEdge(HAS_TAG, tagJava);
        post2.addEdge(HAS_TAG, tagPython);

        Vertex post3 = builder.createPost(382L, "Java and Rust", DATE_2022, newYork, bob);
        post3.addEdge(HAS_TAG, tagJava);
        post3.addEdge(HAS_TAG, tagRust);

        // Carol's post with Java + Go
        Vertex post4 = builder.createPost(383L, "Java and Go", DATE_2022, newYork, carol);
        post4.addEdge(HAS_TAG, tagJava);
        post4.addEdge(HAS_TAG, tagGo);

        // Post with only Java (no co-occurrence)
        Vertex post5 = builder.createPost(384L, "Just Java", DATE_2022, newYork, bob);
        post5.addEdge(HAS_TAG, tagJava);
    }

    @Test
    void testFindCoOccurringTags() {
        ComplexReadQuery6 query = new ComplexReadQuery6();
        LdbcQuery6 operation = new LdbcQuery6(ALICE_ID, "Java", 10);

        List<LdbcQuery6Result> results = executeQuery(query, operation);

        // Should find Python, Rust, Go (co-occurring with Java)
        assertFalse(results.isEmpty(), "Should find co-occurring tags");

        boolean foundPython = results.stream().anyMatch(r -> r.getTagName().equals("Python"));
        boolean foundRust = results.stream().anyMatch(r -> r.getTagName().equals("Rust"));
        boolean foundGo = results.stream().anyMatch(r -> r.getTagName().equals("Go"));

        assertTrue(foundPython, "Python should be found (co-occurs with Java)");
        assertTrue(foundRust, "Rust should be found (co-occurs with Java)");
        assertTrue(foundGo, "Go should be found (co-occurs with Java)");
    }

    @Test
    void testGivenTagExcluded() {
        ComplexReadQuery6 query = new ComplexReadQuery6();
        LdbcQuery6 operation = new LdbcQuery6(ALICE_ID, "Java", 10);

        List<LdbcQuery6Result> results = executeQuery(query, operation);

        // Java itself should NOT be in results
        boolean foundJava = results.stream().anyMatch(r -> r.getTagName().equals("Java"));
        assertFalse(foundJava, "The given tag (Java) should not be in results");
    }

    @Test
    void testCountsAreCorrect() {
        ComplexReadQuery6 query = new ComplexReadQuery6();
        LdbcQuery6 operation = new LdbcQuery6(ALICE_ID, "Java", 10);

        List<LdbcQuery6Result> results = executeQuery(query, operation);

        // Python co-occurs with Java in 2 posts
        LdbcQuery6Result pythonResult = results.stream()
                .filter(r -> r.getTagName().equals("Python"))
                .findFirst()
                .orElse(null);

        assertNotNull(pythonResult, "Should find Python");
        assertEquals(2, pythonResult.getPostCount(), "Python should co-occur with Java in 2 posts");

        // Rust co-occurs with Java in 1 post
        LdbcQuery6Result rustResult = results.stream()
                .filter(r -> r.getTagName().equals("Rust"))
                .findFirst()
                .orElse(null);

        assertNotNull(rustResult, "Should find Rust");
        assertEquals(1, rustResult.getPostCount(), "Rust should co-occur with Java in 1 post");
    }

    @Test
    void testOrderingByCountDescThenNameAsc() {
        ComplexReadQuery6 query = new ComplexReadQuery6();
        LdbcQuery6 operation = new LdbcQuery6(ALICE_ID, "Java", 10);

        List<LdbcQuery6Result> results = executeQuery(query, operation);

        // Verify ordering
        for (int i = 0; i < results.size() - 1; i++) {
            LdbcQuery6Result current = results.get(i);
            LdbcQuery6Result next = results.get(i + 1);

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
        ComplexReadQuery6 query = new ComplexReadQuery6();
        LdbcQuery6 operation = new LdbcQuery6(ALICE_ID, "Java", 2);

        List<LdbcQuery6Result> results = executeQuery(query, operation);

        assertEquals(2, results.size(), "Should be limited to 2 results");
    }

    @Test
    void testNoResultsForNonExistentTag() {
        ComplexReadQuery6 query = new ComplexReadQuery6();
        LdbcQuery6 operation = new LdbcQuery6(ALICE_ID, "NonExistentTag", 10);

        List<LdbcQuery6Result> results = executeQuery(query, operation);

        assertTrue(results.isEmpty(), "Should have no results for non-existent tag");
    }
}
