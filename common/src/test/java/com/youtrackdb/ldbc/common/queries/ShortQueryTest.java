package com.youtrackdb.ldbc.common.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.junit.jupiter.api.Test;
import org.ldbcouncil.snb.driver.workloads.interactive.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ShortQueryTest extends AbstractQueryTest {

    // ==================== IS1: Person Profile ====================

    @Test
    void testShortQuery1_personExists() {
        ShortReadQuery1 query = new ShortReadQuery1();
        LdbcShortQuery1PersonProfile operation = new LdbcShortQuery1PersonProfile(ALICE_ID);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        assertTrue(traversal.hasNext(), "Should find Alice");

        LdbcShortQuery1PersonProfileResult result = query.toResult(traversal.next());

        assertEquals("Alice", result.getFirstName());
        assertEquals("Anderson", result.getLastName());
        assertEquals("female", result.getGender());
        assertEquals(NEW_YORK_ID, result.getCityId());
    }

    @Test
    void testShortQuery1_personNotFound() {
        ShortReadQuery1 query = new ShortReadQuery1();
        LdbcShortQuery1PersonProfile operation = new LdbcShortQuery1PersonProfile(9999L);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        assertFalse(traversal.hasNext(), "Should not find non-existent person");
    }

    // ==================== IS2: Person Posts ====================

    @Test
    void testShortQuery2_personWithPosts() {
        ShortReadQuery2 query = new ShortReadQuery2();
        LdbcShortQuery2PersonPosts operation = new LdbcShortQuery2PersonPosts(ALICE_ID, 10);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        List<LdbcShortQuery2PersonPostsResult> results = new ArrayList<>();
        while (traversal.hasNext()) {
            results.add(query.toResult(traversal.next()));
        }

        assertEquals(2, results.size(), "Alice has 2 posts");
        // Should be ordered by creationDate DESC
        assertEquals(POST2_ID, results.get(0).getMessageId(), "Most recent post first");
        assertEquals(POST1_ID, results.get(1).getMessageId());
    }

    @Test
    void testShortQuery2_personWithNoPosts() {
        ShortReadQuery2 query = new ShortReadQuery2();
        LdbcShortQuery2PersonPosts operation = new LdbcShortQuery2PersonPosts(DAVID_ID, 10);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        assertFalse(traversal.hasNext(), "David has no posts");
    }

    // ==================== IS3: Person Friends ====================

    @Test
    void testShortQuery3_personWithFriends() {
        ShortReadQuery3 query = new ShortReadQuery3();
        LdbcShortQuery3PersonFriends operation = new LdbcShortQuery3PersonFriends(BOB_ID);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        List<LdbcShortQuery3PersonFriendsResult> results = new ArrayList<>();
        while (traversal.hasNext()) {
            results.add(query.toResult(traversal.next()));
        }

        assertEquals(2, results.size(), "Bob knows Alice and Carol");

        List<Long> friendIds = results.stream()
                .map(LdbcShortQuery3PersonFriendsResult::getPersonId)
                .toList();
        assertTrue(friendIds.contains(ALICE_ID));
        assertTrue(friendIds.contains(CAROL_ID));
    }

    // ==================== IS4: Message Content ====================

    @Test
    void testShortQuery4_postContent() {
        ShortReadQuery4 query = new ShortReadQuery4();
        LdbcShortQuery4MessageContent operation = new LdbcShortQuery4MessageContent(POST1_ID);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        assertTrue(traversal.hasNext());

        LdbcShortQuery4MessageContentResult result = query.toResult(traversal.next());
        assertEquals("Hello World!", result.getMessageContent());
    }

    @Test
    void testShortQuery4_commentContent() {
        ShortReadQuery4 query = new ShortReadQuery4();
        LdbcShortQuery4MessageContent operation = new LdbcShortQuery4MessageContent(COMMENT1_ID);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        assertTrue(traversal.hasNext());

        LdbcShortQuery4MessageContentResult result = query.toResult(traversal.next());
        assertEquals("Great post!", result.getMessageContent());
    }

    // ==================== IS5: Message Creator ====================

    @Test
    void testShortQuery5_postCreator() {
        ShortReadQuery5 query = new ShortReadQuery5();
        LdbcShortQuery5MessageCreator operation = new LdbcShortQuery5MessageCreator(POST1_ID);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        assertTrue(traversal.hasNext());

        LdbcShortQuery5MessageCreatorResult result = query.toResult(traversal.next());
        assertEquals(ALICE_ID, result.getPersonId());
        assertEquals("Alice", result.getFirstName());
        assertEquals("Anderson", result.getLastName());
    }

    @Test
    void testShortQuery5_commentCreator() {
        ShortReadQuery5 query = new ShortReadQuery5();
        LdbcShortQuery5MessageCreator operation = new LdbcShortQuery5MessageCreator(COMMENT1_ID);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        assertTrue(traversal.hasNext());

        LdbcShortQuery5MessageCreatorResult result = query.toResult(traversal.next());
        assertEquals(BOB_ID, result.getPersonId());
        assertEquals("Bob", result.getFirstName());
    }

    // ==================== IS6: Message Forum ====================

    @Test
    void testShortQuery6_postForum() {
        ShortReadQuery6 query = new ShortReadQuery6();
        LdbcShortQuery6MessageForum operation = new LdbcShortQuery6MessageForum(POST1_ID);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        assertTrue(traversal.hasNext());

        LdbcShortQuery6MessageForumResult result = query.toResult(traversal.next());
        assertEquals(TECH_TALK_ID, result.getForumId());
        assertEquals("TechTalk", result.getForumTitle());
        assertEquals(ALICE_ID, result.getModeratorId());
    }

    // ==================== IS7: Message Replies ====================

    @Test
    void testShortQuery7_postWithReplies() {
        ShortReadQuery7 query = new ShortReadQuery7();
        LdbcShortQuery7MessageReplies operation = new LdbcShortQuery7MessageReplies(POST1_ID);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        List<LdbcShortQuery7MessageRepliesResult> results = new ArrayList<>();
        while (traversal.hasNext()) {
            results.add(query.toResult(traversal.next()));
        }

        assertEquals(1, results.size(), "Post1 has one reply");
        assertEquals(COMMENT1_ID, results.getFirst().getCommentId());
        assertEquals(BOB_ID, results.getFirst().getReplyAuthorId());
        assertEquals("Bob", results.getFirst().getReplyAuthorFirstName());
    }

    @Test
    void testShortQuery7_postWithNoReplies() {
        ShortReadQuery7 query = new ShortReadQuery7();
        LdbcShortQuery7MessageReplies operation = new LdbcShortQuery7MessageReplies(POST2_ID);

        GraphTraversal<?, Map<String, Object>> traversal = query.buildTraversal(operation, g);
        assertFalse(traversal.hasNext(), "Post2 has no replies");
    }
}
