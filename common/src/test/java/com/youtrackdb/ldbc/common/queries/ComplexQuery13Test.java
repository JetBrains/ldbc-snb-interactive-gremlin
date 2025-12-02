package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery13;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery13Result;

import java.util.List;
import java.util.Map;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * IC13: Single shortest path
 *
 * Given two Persons, find the shortest path between these two Persons in the
 * subgraph induced by the KNOWS relationship.
 *
 * Test graph (base + additional):
 * <pre>
 *     Alice(1) --KNOWS-- Bob(2) --KNOWS-- Carol(3) --KNOWS-- David(4)
 *                          |                                    |
 *                        Eve(5) --KNOWS-- Frank(6) --KNOWS-- Grace(7)
 *
 * Paths from Alice to Grace:
 * - Alice -> Bob -> Carol -> David -> Grace (length 4) - if David-Grace connected
 * - Alice -> Bob -> Eve -> Frank -> Grace (length 4)
 * </pre>
 */
class ComplexQuery13Test extends AbstractQueryTest {

    private static final long EVE_ID = 5L;
    private static final long FRANK_ID = 6L;
    private static final long GRACE_ID = 7L;
    private static final long ISOLATED_ID = 8L;

    @BeforeEach
    void setUpAdditionalData() {
        Vertex newYork = g.V().has(PLACE, ID, NEW_YORK_ID).next();
        Vertex bob = g.V().has(PERSON, ID, BOB_ID).next();
        Vertex david = g.V().has(PERSON, ID, DAVID_ID).next();

        // Create additional persons
        Vertex eve = createPerson(EVE_ID, "Eve", "Edwards", newYork);
        Vertex frank = createPerson(FRANK_ID, "Frank", "Foster", newYork);
        Vertex grace = createPerson(GRACE_ID, "Grace", "Green", newYork);
        Vertex isolated = createPerson(ISOLATED_ID, "Isolated", "Person", newYork);

        // Create KNOWS edges (bidirectional)
        // Bob -- Eve
        bob.addEdge(KNOWS, eve, CREATION_DATE, DATE_2021);
        eve.addEdge(KNOWS, bob, CREATION_DATE, DATE_2021);

        // Eve -- Frank
        eve.addEdge(KNOWS, frank, CREATION_DATE, DATE_2021);
        frank.addEdge(KNOWS, eve, CREATION_DATE, DATE_2021);

        // Frank -- Grace
        frank.addEdge(KNOWS, grace, CREATION_DATE, DATE_2021);
        grace.addEdge(KNOWS, frank, CREATION_DATE, DATE_2021);

        // David -- Grace (creates alternative path)
        david.addEdge(KNOWS, grace, CREATION_DATE, DATE_2022);
        grace.addEdge(KNOWS, david, CREATION_DATE, DATE_2022);

        // Isolated person has no KNOWS edges
    }

    private Vertex createPerson(long id, String firstName, String lastName, Vertex city) {
        Vertex person = g.addV(PERSON)
                .property(ID, id)
                .property(FIRST_NAME, firstName)
                .property(LAST_NAME, lastName)
                .property(GENDER, "male")
                .property(BIRTHDAY, BIRTHDAY_1990)
                .property(CREATION_DATE, DATE_2020)
                .property(LOCATION_IP, "127.0.0.1")
                .property(BROWSER_USED, "Chrome")
                .property(LANGUAGES, List.of("en"))
                .property(EMAILS, List.of(firstName.toLowerCase() + "@example.com"))
                .next();
        person.addEdge(IS_LOCATED_IN, city);
        return person;
    }

    @Test
    void testSamePersonDistance0() {
        ComplexReadQuery13 query = new ComplexReadQuery13();
        LdbcQuery13 operation = new LdbcQuery13(ALICE_ID, ALICE_ID);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        assertTrue(traversal.hasNext(), "Should find path to self");

        LdbcQuery13Result result = query.toResult(traversal.next());
        assertEquals(0, result.getShortestPathLength(), "Distance to self should be 0");
    }

    @Test
    void testDirectFriendDistance1() {
        ComplexReadQuery13 query = new ComplexReadQuery13();
        LdbcQuery13 operation = new LdbcQuery13(ALICE_ID, BOB_ID);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        assertTrue(traversal.hasNext(), "Should find path to direct friend");

        LdbcQuery13Result result = query.toResult(traversal.next());
        assertEquals(1, result.getShortestPathLength(), "Distance to direct friend should be 1");
    }

    @Test
    void testFriendOfFriendDistance2() {
        ComplexReadQuery13 query = new ComplexReadQuery13();
        // Alice -> Bob -> Carol
        LdbcQuery13 operation = new LdbcQuery13(ALICE_ID, CAROL_ID);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        assertTrue(traversal.hasNext(), "Should find path to friend-of-friend");

        LdbcQuery13Result result = query.toResult(traversal.next());
        assertEquals(2, result.getShortestPathLength(), "Distance to friend-of-friend should be 2");
    }

    @Test
    void testDistance3() {
        ComplexReadQuery13 query = new ComplexReadQuery13();
        // Alice -> Bob -> Carol -> David
        LdbcQuery13 operation = new LdbcQuery13(ALICE_ID, DAVID_ID);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        assertTrue(traversal.hasNext(), "Should find path at distance 3");

        LdbcQuery13Result result = query.toResult(traversal.next());
        assertEquals(3, result.getShortestPathLength(), "Distance should be 3");
    }

    @Test
    void testDistance4ViaAlternativePath() {
        ComplexReadQuery13 query = new ComplexReadQuery13();
        // Alice -> Bob -> Eve -> Frank -> Grace (length 4)
        // OR Alice -> Bob -> Carol -> David -> Grace (length 4)
        LdbcQuery13 operation = new LdbcQuery13(ALICE_ID, GRACE_ID);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        assertTrue(traversal.hasNext(), "Should find path at distance 4");

        LdbcQuery13Result result = query.toResult(traversal.next());
        assertEquals(4, result.getShortestPathLength(), "Distance should be 4");
    }

    @Test
    void testNoPathToIsolatedPerson() {
        ComplexReadQuery13 query = new ComplexReadQuery13();
        LdbcQuery13 operation = new LdbcQuery13(ALICE_ID, ISOLATED_ID);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);

        // When no path exists, the query should return -1 or no result
        if (traversal.hasNext()) {
            LdbcQuery13Result result = query.toResult(traversal.next());
            assertEquals(-1, result.getShortestPathLength(),
                    "No path should return -1");
        }
        // Or the traversal simply has no results, which is also acceptable
    }

    @Test
    void testSymmetricPath() {
        ComplexReadQuery13 query = new ComplexReadQuery13();

        // Path from Alice to Carol
        LdbcQuery13 operationAC = new LdbcQuery13(ALICE_ID, CAROL_ID);
        GraphTraversal<?, Map<String, Object>> traversalAC = query.buildTraversal(operationAC, g);
        assertTrue(traversalAC.hasNext());
        int distanceAC = query.toResult(traversalAC.next()).getShortestPathLength();

        // Path from Carol to Alice (should be same distance)
        LdbcQuery13 operationCA = new LdbcQuery13(CAROL_ID, ALICE_ID);
        GraphTraversal<?, Map<String, Object>> traversalCA = query.buildTraversal(operationCA, g);
        assertTrue(traversalCA.hasNext());
        int distanceCA = query.toResult(traversalCA.next()).getShortestPathLength();

        assertEquals(distanceAC, distanceCA,
                "Shortest path should be symmetric (same in both directions)");
    }

    @Test
    void testShortestOfMultiplePaths() {
        // Eve is reachable from Alice via:
        // - Alice -> Bob -> Eve (length 2)
        // There's no shorter path
        ComplexReadQuery13 query = new ComplexReadQuery13();
        LdbcQuery13 operation = new LdbcQuery13(ALICE_ID, EVE_ID);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        assertTrue(traversal.hasNext());

        LdbcQuery13Result result = query.toResult(traversal.next());
        assertEquals(2, result.getShortestPathLength(),
                "Should find shortest path length 2");
    }
}
