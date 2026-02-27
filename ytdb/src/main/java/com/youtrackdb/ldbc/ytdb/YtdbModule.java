package com.youtrackdb.ldbc.ytdb;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.youtrackdb.ldbc.common.GraphProvider;
import com.youtrackdb.ldbc.common.OperationBindings;
import com.youtrackdb.ldbc.ytdb.sql.*;
import org.ldbcouncil.snb.driver.workloads.interactive.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class YtdbModule extends AbstractModule {
    private static final Logger logger = LoggerFactory.getLogger(YtdbModule.class);

    private final Map<String, String> properties;

    public YtdbModule(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    protected void configure() {
        logger.info("Configuring YouTrackDB Guice module");

        bind(new TypeLiteral<Map<String, String>>() {})
                .annotatedWith(Names.named("properties"))
                .toInstance(properties);

        String mode = resolveMode();
        logger.info("YouTrackDB mode: {}", mode);

        if ("embedded".equalsIgnoreCase(mode)) {
            bind(GraphProvider.class).to(YtdbGraphProvider.class);
        } else {
            bind(GraphProvider.class).to(YtdbRemoteGraphProvider.class);
        }

        OperationBindings.bindQuery(this.binder(), LdbcShortQuery1PersonProfile.class, SqlShortQuery1.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery2PersonPosts.class, SqlShortQuery2.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery3PersonFriends.class, SqlShortQuery3.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery4MessageContent.class, SqlShortQuery4.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery5MessageCreator.class, SqlShortQuery5.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery6MessageForum.class, SqlShortQuery6.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery7MessageReplies.class, SqlShortQuery7.class);

        OperationBindings.bindQuery(this.binder(), LdbcQuery1.class, SqlComplexQuery1.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery2.class, SqlComplexQuery2.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery3.class, SqlComplexQuery3.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery4.class, SqlComplexQuery4.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery5.class, SqlComplexQuery5.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery6.class, SqlComplexQuery6.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery7.class, SqlComplexQuery7.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery8.class, SqlComplexQuery8.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery9.class, SqlComplexQuery9.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery10.class, SqlComplexQuery10.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery11.class, SqlComplexQuery11.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery12.class, SqlComplexQuery12.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery13.class, SqlComplexQuery13.class);

        OperationBindings.bindQuery(this.binder(), LdbcUpdate1AddPerson.class, SqlUpdate1.class);
        OperationBindings.bindQuery(this.binder(), LdbcUpdate2AddPostLike.class, SqlUpdate2.class);
        OperationBindings.bindQuery(this.binder(), LdbcUpdate3AddCommentLike.class, SqlUpdate3.class);
        OperationBindings.bindQuery(this.binder(), LdbcUpdate4AddForum.class, SqlUpdate4.class);
        OperationBindings.bindQuery(this.binder(), LdbcUpdate5AddForumMembership.class, SqlUpdate5.class);
        OperationBindings.bindQuery(this.binder(), LdbcUpdate6AddPost.class, SqlUpdate6.class);
        OperationBindings.bindQuery(this.binder(), LdbcUpdate7AddComment.class, SqlUpdate7.class);
        OperationBindings.bindQuery(this.binder(), LdbcUpdate8AddFriendship.class, SqlUpdate8.class);
    }

    private String resolveMode() {
        String env = System.getenv("YTDB_MODE");
        if (env != null && !env.isBlank()) {
            return env;
        }
        String prop = properties.get("ytdb.mode");
        if (prop != null && !prop.isBlank()) {
            return prop;
        }
        return "remote";
    }
}
