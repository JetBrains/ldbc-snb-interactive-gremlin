package com.youtrackdb.ldbc.common.queries;

import org.junit.jupiter.api.Test;
import org.ldbcouncil.snb.driver.workloads.interactive.*;

import java.util.Date;
import java.util.List;

import static com.youtrackdb.ldbc.common.LdbcSchema.*;
import static org.junit.jupiter.api.Assertions.*;

class UpdateQueryTest extends AbstractQueryTest {

    // ==================== INS1: Add Person ====================

    @Test
    void testUpdate1_addPerson() {
        Update1AddPerson update = new Update1AddPerson();
        long newPersonId = 999L;

        LdbcUpdate1AddPerson operation = new LdbcUpdate1AddPerson(
                newPersonId,
                "Eve",
                "Evans",
                "female",
                new Date(),
                new Date(),
                "192.168.1.1",
                "Firefox",
                NEW_YORK_ID,
                List.of("en", "fr"),
                List.of("eve@example.com"),
                List.of(TAG_JAVA_ID),
                List.of(), // studyAt
                List.of()  // workAt
        );

        update.executeUpdate(operation, g);

        // Verify person was created
        assertTrue(g.V().has(PERSON, ID, newPersonId).hasNext(), "Person should exist");
        assertEquals("Eve", g.V().has(PERSON, ID, newPersonId).values(FIRST_NAME).next());
        assertEquals("Evans", g.V().has(PERSON, ID, newPersonId).values(LAST_NAME).next());

        // Verify location edge
        assertTrue(g.V().has(PERSON, ID, newPersonId).out(IS_LOCATED_IN).has(ID, NEW_YORK_ID).hasNext(),
                "Should be located in New York");

        // Verify interest edge
        assertTrue(g.V().has(PERSON, ID, newPersonId).out(HAS_INTEREST).has(ID, TAG_JAVA_ID).hasNext(),
                "Should have interest in Java tag");
    }

    // ==================== INS2: Add Post Like ====================

    @Test
    void testUpdate2_addPostLike() {
        Update2AddPostLike update = new Update2AddPostLike();

        // Carol likes Post2 (she hasn't liked it yet in fixture)
        LdbcUpdate2AddPostLike operation = new LdbcUpdate2AddPostLike(
                CAROL_ID,
                POST2_ID,
                new Date()
        );

        // Verify Carol doesn't already like Post2
        assertFalse(g.V().has(PERSON, ID, CAROL_ID).out(LIKES).has(ID, POST2_ID).hasNext());

        update.executeUpdate(operation, g);

        // Verify like was created
        assertTrue(g.V().has(PERSON, ID, CAROL_ID).out(LIKES).has(ID, POST2_ID).hasNext(),
                "Carol should now like Post2");
    }

    // ==================== INS3: Add Comment Like ====================

    @Test
    void testUpdate3_addCommentLike() {
        Update3AddCommentLike update = new Update3AddCommentLike();

        // Alice likes Comment1
        LdbcUpdate3AddCommentLike operation = new LdbcUpdate3AddCommentLike(
                ALICE_ID,
                COMMENT1_ID,
                new Date()
        );

        update.executeUpdate(operation, g);

        // Verify like was created
        assertTrue(g.V().has(PERSON, ID, ALICE_ID).out(LIKES).has(ID, COMMENT1_ID).hasNext(),
                "Alice should now like Comment1");
    }

    // ==================== INS4: Add Forum ====================

    @Test
    void testUpdate4_addForum() {
        Update4AddForum update = new Update4AddForum();
        long newForumId = 999L;

        LdbcUpdate4AddForum operation = new LdbcUpdate4AddForum(
                newForumId,
                "New Forum",
                new Date(),
                BOB_ID, // moderator
                List.of(TAG_DB_ID)
        );

        update.executeUpdate(operation, g);

        // Verify forum was created
        assertTrue(g.V().has(FORUM, ID, newForumId).hasNext(), "Forum should exist");
        assertEquals("New Forum", g.V().has(FORUM, ID, newForumId).values(TITLE).next());

        // Verify moderator edge
        assertTrue(g.V().has(FORUM, ID, newForumId).out(HAS_MODERATOR).has(ID, BOB_ID).hasNext(),
                "Bob should be moderator");

        // Verify tag edge
        assertTrue(g.V().has(FORUM, ID, newForumId).out(HAS_TAG).has(ID, TAG_DB_ID).hasNext(),
                "Forum should have Databases tag");
    }

    // ==================== INS5: Add Forum Membership ====================

    @Test
    void testUpdate5_addForumMembership() {
        Update5AddForumMembership update = new Update5AddForumMembership();

        // Carol joins TechTalk forum
        LdbcUpdate5AddForumMembership operation = new LdbcUpdate5AddForumMembership(
                TECH_TALK_ID,
                CAROL_ID,
                new Date()
        );

        // Verify Carol isn't already a member
        assertFalse(g.V().has(FORUM, ID, TECH_TALK_ID).out(HAS_MEMBER).has(ID, CAROL_ID).hasNext());

        update.executeUpdate(operation, g);

        // Verify membership was created
        assertTrue(g.V().has(FORUM, ID, TECH_TALK_ID).out(HAS_MEMBER).has(ID, CAROL_ID).hasNext(),
                "Carol should be a member of TechTalk");
    }

    // ==================== INS6: Add Post ====================

    @Test
    void testUpdate6_addPost() {
        Update6AddPost update = new Update6AddPost();
        long newPostId = 999L;

        LdbcUpdate6AddPost operation = new LdbcUpdate6AddPost(
                newPostId,
                "", // imageFile
                new Date(),
                "192.168.1.1",
                "Chrome",
                "en",
                "This is a new post!",
                21, // length
                BOB_ID, // author
                TECH_TALK_ID, // forum
                USA_ID, // location
                List.of(TAG_JAVA_ID)
        );

        update.executeUpdate(operation, g);

        // Verify post was created
        assertTrue(g.V().has(POST, ID, newPostId).hasNext(), "Post should exist");
        assertEquals("This is a new post!", g.V().has(POST, ID, newPostId).values(CONTENT).next());

        // Verify creator edge
        assertTrue(g.V().has(POST, ID, newPostId).out(HAS_CREATOR).has(ID, BOB_ID).hasNext(),
                "Bob should be creator");

        // Verify forum container edge
        assertTrue(g.V().has(FORUM, ID, TECH_TALK_ID).out(CONTAINER_OF).has(ID, newPostId).hasNext(),
                "Post should be in TechTalk forum");

        // Verify tag edge
        assertTrue(g.V().has(POST, ID, newPostId).out(HAS_TAG).has(ID, TAG_JAVA_ID).hasNext(),
                "Post should have Java tag");
    }

    // ==================== INS7: Add Comment ====================

    @Test
    void testUpdate7_addComment() {
        Update7AddComment update = new Update7AddComment();
        long newCommentId = 999L;

        LdbcUpdate7AddComment operation = new LdbcUpdate7AddComment(
                newCommentId,
                new Date(),
                "192.168.1.1",
                "Safari",
                "Nice comment!",
                13, // length
                CAROL_ID, // author
                USA_ID, // location
                POST1_ID, // replyToPost
                -1L, // replyToComment (-1 means replying to post, not comment)
                List.of(TAG_DB_ID)
        );

        update.executeUpdate(operation, g);

        // Verify comment was created
        assertTrue(g.V().has(COMMENT, ID, newCommentId).hasNext(), "Comment should exist");
        assertEquals("Nice comment!", g.V().has(COMMENT, ID, newCommentId).values(CONTENT).next());

        // Verify creator edge
        assertTrue(g.V().has(COMMENT, ID, newCommentId).out(HAS_CREATOR).has(ID, CAROL_ID).hasNext(),
                "Carol should be creator");

        // Verify reply edge to post
        assertTrue(g.V().has(COMMENT, ID, newCommentId).out(REPLY_OF).has(ID, POST1_ID).hasNext(),
                "Comment should reply to Post1");
    }

    @Test
    void testUpdate7_addCommentReplyToComment() {
        Update7AddComment update = new Update7AddComment();
        long newCommentId = 998L;

        // Reply to Comment1 (not to a post)
        LdbcUpdate7AddComment operation = new LdbcUpdate7AddComment(
                newCommentId,
                new Date(),
                "192.168.1.1",
                "Safari",
                "Reply to comment!",
                17,
                DAVID_ID,
                UK_ID,
                -1L, // replyToPost (-1 means replying to comment)
                COMMENT1_ID, // replyToComment
                List.of()
        );

        update.executeUpdate(operation, g);

        // Verify reply edge to comment
        assertTrue(g.V().has(COMMENT, ID, newCommentId).out(REPLY_OF).has(ID, COMMENT1_ID).hasNext(),
                "Comment should reply to Comment1");
    }

    // ==================== INS8: Add Friendship ====================

    @Test
    void testUpdate8_addFriendship() {
        Update8AddFriendship update = new Update8AddFriendship();

        // Alice and David become friends (they're not directly connected in fixture)
        LdbcUpdate8AddFriendship operation = new LdbcUpdate8AddFriendship(
                ALICE_ID,
                DAVID_ID,
                new Date()
        );

        // Verify they're not already friends
        assertFalse(g.V().has(PERSON, ID, ALICE_ID).out(KNOWS).has(ID, DAVID_ID).hasNext());

        update.executeUpdate(operation, g);

        assertTrue(g.V().has(PERSON, ID, ALICE_ID).out(KNOWS).has(ID, DAVID_ID).hasNext(),
                "Alice should know David");
        assertTrue(g.V().has(PERSON, ID, DAVID_ID).out(KNOWS).has(ID, ALICE_ID).hasNext(),
                "David should know Alice (bidirectional)");

        // Verify both(KNOWS) traversal works correctly
        assertTrue(g.V().has(PERSON, ID, ALICE_ID).both(KNOWS).has(ID, DAVID_ID).hasNext(),
                "both(KNOWS) traversal should find David from Alice");
        assertTrue(g.V().has(PERSON, ID, DAVID_ID).both(KNOWS).has(ID, ALICE_ID).hasNext(),
                "both(KNOWS) traversal should find Alice from David");
    }
}
