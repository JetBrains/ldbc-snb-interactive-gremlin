package com.youtrackdb.ldbc.common;

import com.google.inject.AbstractModule;
import com.youtrackdb.ldbc.common.queries.*;
import org.ldbcouncil.snb.driver.workloads.interactive.*;

public class DefaultQueryModule extends AbstractModule {

    @Override
    protected void configure() {
        // ==================== SHORT QUERIES (IS1-IS7) ====================
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery1PersonProfile.class, ShortReadQuery1.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery2PersonPosts.class, ShortReadQuery2.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery3PersonFriends.class, ShortReadQuery3.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery4MessageContent.class, ShortReadQuery4.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery5MessageCreator.class, ShortReadQuery5.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery6MessageForum.class, ShortReadQuery6.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery7MessageReplies.class, ShortReadQuery7.class);

        // ==================== COMPLEX QUERIES (IC1-IC14) ====================
        OperationBindings.bindQuery(this.binder(), LdbcQuery1.class, ComplexReadQuery1.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery2.class, ComplexReadQuery2.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery3.class, ComplexReadQuery3.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery4.class, ComplexReadQuery4.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery5.class, ComplexReadQuery5.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery6.class, ComplexReadQuery6.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery7.class, ComplexReadQuery7.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery8.class, ComplexReadQuery8.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery9.class, ComplexReadQuery9.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery10.class, ComplexReadQuery10.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery11.class, ComplexReadQuery11.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery12.class, ComplexReadQuery12.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery13.class, ComplexReadQuery13.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery14.class, ComplexReadQuery14.class);

        // ==================== UPDATE OPERATIONS (INS1-INS8) ====================
        OperationBindings.bindQuery(this.binder(), LdbcUpdate1AddPerson.class, Update1AddPerson.class);
        OperationBindings.bindQuery(this.binder(), LdbcUpdate2AddPostLike.class, Update2AddPostLike.class);
        OperationBindings.bindQuery(this.binder(), LdbcUpdate3AddCommentLike.class, Update3AddCommentLike.class);
        OperationBindings.bindQuery(this.binder(), LdbcUpdate4AddForum.class, Update4AddForum.class);
        OperationBindings.bindQuery(this.binder(), LdbcUpdate5AddForumMembership.class, Update5AddForumMembership.class);
        OperationBindings.bindQuery(this.binder(), LdbcUpdate6AddPost.class, Update6AddPost.class);
        OperationBindings.bindQuery(this.binder(), LdbcUpdate7AddComment.class, Update7AddComment.class);
        OperationBindings.bindQuery(this.binder(), LdbcUpdate8AddFriendship.class, Update8AddFriendship.class);
    }
}
