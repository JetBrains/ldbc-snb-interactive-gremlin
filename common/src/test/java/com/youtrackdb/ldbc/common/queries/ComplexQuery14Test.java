package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery14;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery14Result;

import java.util.ArrayList;
import java.util.List;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * IC14: Trusted connection paths
 *
 * Given two Persons, find all (unweighted) shortest paths between these two Persons in the
 * subgraph induced by the knows relationship. Then, for each path calculate a weight.
 * The weight of a path is the sum of weights between every pair of consecutive Person nodes.
 * - Every direct reply to a Post is 1.0
 * - Every direct reply to a Comment is 0.5
 */
class ComplexQuery14Test extends AbstractQueryTest {

    @BeforeEach
    void setUpAdditionalData() {
        TestDataBuilder builder = builder();

        Vertex alice = g.V().has(PERSON, ID, ALICE_ID).next();
        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();
        Vertex carol = g.V().has(PERSON, ID, CAROL_ID).next();
        Vertex newYork = g.V().has(PLACE, ID, NEW_YORK_ID).next();

        // Alice's post
        Vertex alicePost = builder.createPost(398L, "Alice's post for IC14", DATE_2022, newYork, alice);

        // Bob replies to Alice's post (weight = 1.0 for post reply)
        Vertex bobReplyToPost = builder.createComment(480L, "Bob replies to Alice's post", DATE_2022, newYork, bob);
        bobReplyToPost.addEdge(REPLY_OF, alicePost);

        // Alice replies to Bob's comment (weight = 0.5 for comment reply)
        Vertex aliceReplyToComment = builder.createComment(481L, "Alice replies to Bob's comment", DATE_2022, newYork, alice);
        aliceReplyToComment.addEdge(REPLY_OF, bobReplyToPost);

        // Bob's post
        Vertex bobPost = builder.createPost(399L, "Bob's post for IC14", DATE_2022, newYork, bob);

        // Carol replies to Bob's post
        Vertex carolReplyToBob = builder.createComment(482L, "Carol replies to Bob", DATE_2022, newYork, carol);
        carolReplyToBob.addEdge(REPLY_OF, bobPost);

        // Bob replies to Carol's comment
        Vertex bobReplyToCarol = builder.createComment(483L, "Bob replies to Carol", DATE_2022, newYork, bob);
        bobReplyToCarol.addEdge(REPLY_OF, carolReplyToBob);
    }

    @Test
    void testFindShortestPath() {
        ComplexReadQuery14 query = new ComplexReadQuery14();
        LdbcQuery14 operation = new LdbcQuery14(ALICE_ID, BOB_ID);

        List<LdbcQuery14Result> results = executeQuery(query, operation);

        // Alice and Bob are directly connected (distance 1)
        assertFalse(results.isEmpty(), "Should find path between Alice and Bob");

        LdbcQuery14Result result = results.get(0);
        List<Long> personIds = toList(result.getPersonIdsInPath());
        assertEquals(2, personIds.size(), "Path should have 2 persons");
        assertTrue(personIds.contains(ALICE_ID), "Path should include Alice");
        assertTrue(personIds.contains(BOB_ID), "Path should include Bob");
    }

    @Test
    void testPathWeight() {
        ComplexReadQuery14 query = new ComplexReadQuery14();
        LdbcQuery14 operation = new LdbcQuery14(ALICE_ID, BOB_ID);

        List<LdbcQuery14Result> results = executeQuery(query, operation);

        assertFalse(results.isEmpty());

        LdbcQuery14Result result = results.get(0);
        // Weight should be positive (interactions exist between Alice and Bob)
        assertTrue(result.getPathWeight() > 0,
                "Path weight should be positive when interactions exist");
    }

    @Test
    void testLongerPath() {
        ComplexReadQuery14 query = new ComplexReadQuery14();
        // Alice to Carol: Alice -> Bob -> Carol (distance 2)
        LdbcQuery14 operation = new LdbcQuery14(ALICE_ID, CAROL_ID);

        List<LdbcQuery14Result> results = executeQuery(query, operation);

        assertFalse(results.isEmpty(), "Should find path between Alice and Carol");

        LdbcQuery14Result result = results.get(0);
        List<Long> personIds = toList(result.getPersonIdsInPath());
        assertEquals(3, personIds.size(), "Path should have 3 persons");
        assertTrue(personIds.contains(ALICE_ID), "Path should include Alice");
        assertTrue(personIds.contains(BOB_ID), "Path should include Bob");
        assertTrue(personIds.contains(CAROL_ID), "Path should include Carol");
    }

    @Test
    void testPathToDistance3() {
        ComplexReadQuery14 query = new ComplexReadQuery14();
        // Alice to David: Alice -> Bob -> Carol -> David (distance 3)
        LdbcQuery14 operation = new LdbcQuery14(ALICE_ID, DAVID_ID);

        List<LdbcQuery14Result> results = executeQuery(query, operation);

        assertFalse(results.isEmpty(), "Should find path between Alice and David");

        LdbcQuery14Result result = results.get(0);
        List<Long> personIds = toList(result.getPersonIdsInPath());
        assertEquals(4, personIds.size(), "Path should have 4 persons");
    }

    @Test
    void testNoPathToDisconnectedPerson() {
        TestDataBuilder builder = builder();
        Vertex newYork = g.V().has(PLACE, ID, NEW_YORK_ID).next();

        // Create an isolated person
        builder.createPerson(999L, "Isolated", "Person", "male", newYork);

        ComplexReadQuery14 query = new ComplexReadQuery14();
        LdbcQuery14 operation = new LdbcQuery14(ALICE_ID, 999L);

        List<LdbcQuery14Result> results = executeQuery(query, operation);

        // No path to isolated person
        assertTrue(results.isEmpty(), "Should have no path to disconnected person");
    }

    @Test
    void testResultsOrderedByWeightDesc() {
        ComplexReadQuery14 query = new ComplexReadQuery14();
        LdbcQuery14 operation = new LdbcQuery14(ALICE_ID, BOB_ID);

        List<LdbcQuery14Result> results = executeQuery(query, operation);

        // If multiple paths exist, they should be ordered by weight DESC
        for (int i = 0; i < results.size() - 1; i++) {
            assertTrue(results.get(i).getPathWeight() >= results.get(i + 1).getPathWeight(),
                    "Results should be ordered by weight DESC");
        }
    }

    @Test
    void testSamePersonPath() {
        ComplexReadQuery14 query = new ComplexReadQuery14();
        // Same start and end person
        LdbcQuery14 operation = new LdbcQuery14(ALICE_ID, ALICE_ID);

        List<LdbcQuery14Result> results = executeQuery(query, operation);

        // Path to self - implementation may return empty or single-node path
        // This depends on implementation choice
        if (!results.isEmpty()) {
            LdbcQuery14Result result = results.get(0);
            List<Long> personIds = toList(result.getPersonIdsInPath());
            assertTrue(personIds.size() <= 1,
                    "Path to self should be empty or single node");
        }
    }

    private List<Long> toList(Iterable<? extends Number> iterable) {
        List<Long> list = new ArrayList<>();
        for (Number n : iterable) {
            list.add(n.longValue());
        }
        return list;
    }
}
