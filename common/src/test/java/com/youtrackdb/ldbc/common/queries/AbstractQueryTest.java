package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.ldbcouncil.snb.driver.Operation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;

/**
 * Base class for query tests using TinkerGraph.
 * Sets up a small LDBC SNB-like graph for testing.
 *
 * Graph structure:
 * <pre>
 * Persons: Alice (1), Bob (2), Carol (3), David (4)
 * Places: New York (100), USA (101), London (102), UK (103)
 * Forums: TechTalk (200)
 * Posts: Post1 (300), Post2 (301)
 * Comments: Comment1 (400)
 * Tags: Java (500), Databases (501)
 * Organisations: ACME Corp (600), MIT (601)
 *
 * Relationships:
 * - Alice KNOWS Bob (bidirectional)
 * - Bob KNOWS Carol (bidirectional)
 * - Carol KNOWS David (bidirectional)
 * - Alice IS_LOCATED_IN New York
 * - Bob IS_LOCATED_IN London
 * - New York IS_PART_OF USA
 * - London IS_PART_OF UK
 * - Alice created Post1, Post2
 * - Bob created Comment1 (reply to Post1)
 * - TechTalk forum contains Post1, Post2
 * - Alice moderates TechTalk
 * - Bob is member of TechTalk
 * </pre>
 */
public abstract class AbstractQueryTest {

    protected Graph graph;
    protected GraphTraversalSource g;

    // Person IDs
    protected static final long ALICE_ID = 1L;
    protected static final long BOB_ID = 2L;
    protected static final long CAROL_ID = 3L;
    protected static final long DAVID_ID = 4L;

    // Place IDs
    protected static final long NEW_YORK_ID = 100L;
    protected static final long USA_ID = 101L;
    protected static final long LONDON_ID = 102L;
    protected static final long UK_ID = 103L;

    // Forum IDs
    protected static final long TECH_TALK_ID = 200L;

    // Post IDs
    protected static final long POST1_ID = 300L;
    protected static final long POST2_ID = 301L;

    // Comment IDs
    protected static final long COMMENT1_ID = 400L;

    // Tag IDs
    protected static final long TAG_JAVA_ID = 500L;
    protected static final long TAG_DB_ID = 501L;

    // Organisation IDs
    protected static final long ACME_ID = 600L;
    protected static final long MIT_ID = 601L;

    // Fixed dates for deterministic testing
    protected static final Date DATE_2020 = new Date(1577836800000L); // 2020-01-01
    protected static final Date DATE_2021 = new Date(1609459200000L); // 2021-01-01
    protected static final Date DATE_2022 = new Date(1640995200000L); // 2022-01-01
    protected static final Date DATE_2023 = new Date(1672531200000L); // 2023-01-01
    protected static final Date BIRTHDAY_1990 = new Date(631152000000L); // 1990-01-01
    protected static final Date BIRTHDAY_1985 = new Date(473385600000L); // 1985-01-01

    @BeforeEach
    void setUp() {
        graph = TinkerGraph.open();
        g = graph.traversal();
        createTestData();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (g != null) {
            g.close();
        }
        if (graph != null) {
            graph.close();
        }
    }

    private void createTestData() {
        // Create Places
        Vertex newYork = createPlace(NEW_YORK_ID, "New York", "City");
        Vertex usa = createPlace(USA_ID, "United States", "Country");
        Vertex london = createPlace(LONDON_ID, "London", "City");
        Vertex uk = createPlace(UK_ID, "United Kingdom", "Country");

        // Place hierarchy
        newYork.addEdge(IS_PART_OF, usa);
        london.addEdge(IS_PART_OF, uk);

        // Create Tags
        Vertex tagJava = createTag(TAG_JAVA_ID, "Java");
        Vertex tagDb = createTag(TAG_DB_ID, "Databases");

        // Create Organisations
        Vertex acme = createOrganisation(ACME_ID, "ACME Corp", "Company");
        Vertex mit = createOrganisation(MIT_ID, "MIT", "University");
        acme.addEdge(IS_LOCATED_IN, usa);
        mit.addEdge(IS_LOCATED_IN, usa);

        // Create Persons
        Vertex alice = createPerson(ALICE_ID, "Alice", "Anderson", "female", BIRTHDAY_1990, DATE_2020);
        Vertex bob = createPerson(BOB_ID, "Bob", "Brown", "male", BIRTHDAY_1985, DATE_2020);
        Vertex carol = createPerson(CAROL_ID, "Carol", "Clark", "female", BIRTHDAY_1990, DATE_2021);
        Vertex david = createPerson(DAVID_ID, "David", "Davis", "male", BIRTHDAY_1985, DATE_2021);

        // Person locations
        alice.addEdge(IS_LOCATED_IN, newYork);
        bob.addEdge(IS_LOCATED_IN, london);
        carol.addEdge(IS_LOCATED_IN, newYork);
        david.addEdge(IS_LOCATED_IN, london);

        // KNOWS relationships (bidirectional as per LDBC spec)
        alice.addEdge(KNOWS, bob, CREATION_DATE, DATE_2020);
        bob.addEdge(KNOWS, alice, CREATION_DATE, DATE_2020);

        bob.addEdge(KNOWS, carol, CREATION_DATE, DATE_2021);
        carol.addEdge(KNOWS, bob, CREATION_DATE, DATE_2021);

        carol.addEdge(KNOWS, david, CREATION_DATE, DATE_2022);
        david.addEdge(KNOWS, carol, CREATION_DATE, DATE_2022);

        // Person interests
        alice.addEdge(HAS_INTEREST, tagJava);
        alice.addEdge(HAS_INTEREST, tagDb);
        bob.addEdge(HAS_INTEREST, tagJava);

        // Work/Study relationships
        alice.addEdge(WORK_AT, acme, WORK_FROM, 2015);
        bob.addEdge(STUDY_AT, mit, CLASS_YEAR, 2010);

        // Create Forum
        Vertex techTalk = createForum(TECH_TALK_ID, "TechTalk", DATE_2020);
        techTalk.addEdge(HAS_MODERATOR, alice);
        techTalk.addEdge(HAS_MEMBER, alice, JOIN_DATE, DATE_2020);
        techTalk.addEdge(HAS_MEMBER, bob, JOIN_DATE, DATE_2021);
        techTalk.addEdge(HAS_TAG, tagJava);

        // Create Posts
        Vertex post1 = createPost(POST1_ID, "Hello World!", DATE_2021, newYork);
        Vertex post2 = createPost(POST2_ID, "TinkerPop rocks!", DATE_2022, newYork);

        post1.addEdge(HAS_CREATOR, alice);
        post2.addEdge(HAS_CREATOR, alice);
        post1.addEdge(HAS_TAG, tagJava);
        post2.addEdge(HAS_TAG, tagDb);

        techTalk.addEdge(CONTAINER_OF, post1);
        techTalk.addEdge(CONTAINER_OF, post2);

        // Create Comment (Bob replies to Alice's post)
        Vertex comment1 = createComment(COMMENT1_ID, "Great post!", DATE_2022, london);
        comment1.addEdge(HAS_CREATOR, bob);
        comment1.addEdge(REPLY_OF, post1);

        // Likes
        bob.addEdge(LIKES, post1, CREATION_DATE, DATE_2022);
        carol.addEdge(LIKES, post1, CREATION_DATE, DATE_2022);
    }

    private Vertex createPerson(long id, String firstName, String lastName, String gender,
                                 Date birthday, Date creationDate) {
        return g.addV(PERSON)
                .property(ID, id)
                .property(FIRST_NAME, firstName)
                .property(LAST_NAME, lastName)
                .property(GENDER, gender)
                .property(BIRTHDAY, birthday)
                .property(CREATION_DATE, creationDate)
                .property(LOCATION_IP, "127.0.0.1")
                .property(BROWSER_USED, "Chrome")
                .property(LANGUAGES, List.of("en"))
                .property(EMAILS, List.of(firstName.toLowerCase() + "@example.com"))
                .next();
    }

    private Vertex createPlace(long id, String name, String type) {
        return g.addV(PLACE)
                .property(ID, id)
                .property(NAME, name)
                .property(TYPE, type)
                .property(URL, "http://example.com/" + name.toLowerCase().replace(" ", "-"))
                .next();
    }

    private Vertex createTag(long id, String name) {
        return g.addV(TAG)
                .property(ID, id)
                .property(NAME, name)
                .property(URL, "http://example.com/tag/" + name.toLowerCase())
                .next();
    }

    private Vertex createOrganisation(long id, String name, String type) {
        return g.addV(ORGANISATION)
                .property(ID, id)
                .property(NAME, name)
                .property(TYPE, type)
                .property(URL, "http://example.com/org/" + name.toLowerCase().replace(" ", "-"))
                .next();
    }

    private Vertex createForum(long id, String title, Date creationDate) {
        return g.addV(FORUM)
                .property(ID, id)
                .property(TITLE, title)
                .property(CREATION_DATE, creationDate)
                .next();
    }

    private Vertex createPost(long id, String content, Date creationDate, Vertex location) {
        Vertex post = g.addV(POST)
                .property(ID, id)
                .property(CONTENT, content)
                .property(CREATION_DATE, creationDate)
                .property(LOCATION_IP, "127.0.0.1")
                .property(BROWSER_USED, "Chrome")
                .property(LANGUAGE, "en")
                .property(LENGTH, content.length())
                .next();
        post.addEdge(IS_LOCATED_IN, location);
        return post;
    }

    private Vertex createComment(long id, String content, Date creationDate, Vertex location) {
        Vertex comment = g.addV(COMMENT)
                .property(ID, id)
                .property(CONTENT, content)
                .property(CREATION_DATE, creationDate)
                .property(LOCATION_IP, "127.0.0.1")
                .property(BROWSER_USED, "Chrome")
                .property(LENGTH, content.length())
                .next();
        comment.addEdge(IS_LOCATED_IN, location);
        return comment;
    }

    protected <TOp extends Operation<List<TResult>>, TResult> List<TResult> executeQuery(
            ListQueryHandler<TOp, TResult> query, TOp operation) {
        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        List<TResult> results = new ArrayList<>();
        while (traversal.hasNext()) {
            results.add(query.toResult(traversal.next()));
        }
        return results;
    }

    protected class TestDataBuilder {

        public Vertex createPerson(long id, String firstName, String lastName, String gender,
                                   Date birthday, Date creationDate, Vertex city) {
            Vertex person = g.addV(PERSON)
                    .property(ID, id)
                    .property(FIRST_NAME, firstName)
                    .property(LAST_NAME, lastName)
                    .property(GENDER, gender)
                    .property(BIRTHDAY, birthday)
                    .property(CREATION_DATE, creationDate)
                    .property(LOCATION_IP, "127.0.0.1")
                    .property(BROWSER_USED, "Chrome")
                    .property(LANGUAGES, List.of("en"))
                    .property(EMAILS, List.of(firstName.toLowerCase() + "@example.com"))
                    .next();
            person.addEdge(IS_LOCATED_IN, city);
            return person;
        }

        public Vertex createPerson(long id, String firstName, String lastName, String gender, Vertex city) {
            return createPerson(id, firstName, lastName, gender, BIRTHDAY_1990, DATE_2020, city);
        }

        public Vertex createPost(long id, String content, Date creationDate, Vertex location) {
            Vertex post = g.addV(POST)
                    .property(ID, id)
                    .property(CONTENT, content)
                    .property(CREATION_DATE, creationDate)
                    .property(LOCATION_IP, "127.0.0.1")
                    .property(BROWSER_USED, "Chrome")
                    .property(LANGUAGE, "en")
                    .property(LENGTH, content.length())
                    .next();
            post.addEdge(IS_LOCATED_IN, location);
            return post;
        }

        public Vertex createPost(long id, String content, Date creationDate, Vertex location, Vertex creator) {
            Vertex post = createPost(id, content, creationDate, location);
            post.addEdge(HAS_CREATOR, creator);
            return post;
        }

        public Vertex createComment(long id, String content, Date creationDate, Vertex location) {
            Vertex comment = g.addV(COMMENT)
                    .property(ID, id)
                    .property(CONTENT, content)
                    .property(CREATION_DATE, creationDate)
                    .property(LOCATION_IP, "127.0.0.1")
                    .property(BROWSER_USED, "Chrome")
                    .property(LENGTH, content.length())
                    .next();
            comment.addEdge(IS_LOCATED_IN, location);
            return comment;
        }

        public Vertex createComment(long id, String content, Date creationDate, Vertex location, Vertex creator) {
            Vertex comment = createComment(id, content, creationDate, location);
            comment.addEdge(HAS_CREATOR, creator);
            return comment;
        }

        public Vertex createForum(long id, String title, Date creationDate) {
            return g.addV(FORUM)
                    .property(ID, id)
                    .property(TITLE, title)
                    .property(CREATION_DATE, creationDate)
                    .next();
        }

        public Vertex createPlace(long id, String name, String type) {
            return g.addV(PLACE)
                    .property(ID, id)
                    .property(NAME, name)
                    .property(TYPE, type)
                    .property(URL, "http://example.com/" + name.toLowerCase().replace(" ", "-"))
                    .next();
        }

        public Vertex createTag(long id, String name) {
            return g.addV(TAG)
                    .property(ID, id)
                    .property(NAME, name)
                    .property(URL, "http://example.com/tag/" + name.toLowerCase())
                    .next();
        }

        public Vertex createOrganisation(long id, String name, String type) {
            return g.addV(ORGANISATION)
                    .property(ID, id)
                    .property(NAME, name)
                    .property(TYPE, type)
                    .property(URL, "http://example.com/org/" + name.toLowerCase().replace(" ", "-"))
                    .next();
        }

        public void createKnowsRelationship(Vertex person1, Vertex person2, Date creationDate) {
            person1.addEdge(KNOWS, person2, CREATION_DATE, creationDate);
            person2.addEdge(KNOWS, person1, CREATION_DATE, creationDate);
        }
    }

    protected TestDataBuilder builder() {
        return new TestDataBuilder();
    }
}
