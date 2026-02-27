package com.youtrackdb.ldbc.ytdb;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.youtrackdb.ldbc.common.GraphProvider;
import com.youtrackdb.ldbc.common.OperationBindings;
import com.youtrackdb.ldbc.ytdb.sql.SqlShortQuery1;
import com.youtrackdb.ldbc.ytdb.sql.SqlShortQuery2;
import com.youtrackdb.ldbc.ytdb.sql.SqlShortQuery3;
import com.youtrackdb.ldbc.ytdb.sql.SqlShortQuery4;
import com.youtrackdb.ldbc.ytdb.sql.SqlShortQuery5;
import com.youtrackdb.ldbc.ytdb.sql.SqlShortQuery6;
import com.youtrackdb.ldbc.ytdb.sql.SqlShortQuery7;
import com.youtrackdb.ldbc.ytdb.sql.SqlComplexQuery1;
import com.youtrackdb.ldbc.ytdb.sql.SqlComplexQuery10;
import com.youtrackdb.ldbc.ytdb.sql.SqlComplexQuery11;
import com.youtrackdb.ldbc.ytdb.sql.SqlComplexQuery12;
import com.youtrackdb.ldbc.ytdb.sql.SqlComplexQuery13;
import com.youtrackdb.ldbc.ytdb.sql.SqlComplexQuery2;
import com.youtrackdb.ldbc.ytdb.sql.SqlComplexQuery3;
import com.youtrackdb.ldbc.ytdb.sql.SqlComplexQuery4;
import com.youtrackdb.ldbc.ytdb.sql.SqlComplexQuery5;
import com.youtrackdb.ldbc.ytdb.sql.SqlComplexQuery6;
import com.youtrackdb.ldbc.ytdb.sql.SqlComplexQuery7;
import com.youtrackdb.ldbc.ytdb.sql.SqlComplexQuery8;
import com.youtrackdb.ldbc.ytdb.sql.SqlComplexQuery9;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery1;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery10;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery11;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery12;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery13;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery2;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery3;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery4;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery5;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery6;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery7;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery8;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery9;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery1PersonProfile;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery2PersonPosts;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery3PersonFriends;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery4MessageContent;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery5MessageCreator;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery6MessageForum;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery7MessageReplies;
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
