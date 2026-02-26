package com.youtrackdb.ldbc.ytdb;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.youtrackdb.ldbc.common.GraphProvider;
import com.youtrackdb.ldbc.common.OperationBindings;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineShortQuery1;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineShortQuery2;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineShortQuery3;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineShortQuery4;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineShortQuery5;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineShortQuery6;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineShortQuery7;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineComplexQuery1;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineComplexQuery10;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineComplexQuery11;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineComplexQuery12;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineComplexQuery13;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineComplexQuery2;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineComplexQuery3;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineComplexQuery4;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineComplexQuery5;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineComplexQuery6;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineComplexQuery7;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineComplexQuery8;
import com.youtrackdb.ldbc.ytdb.baseline.BaselineComplexQuery9;
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

        OperationBindings.bindQuery(this.binder(), LdbcShortQuery1PersonProfile.class, BaselineShortQuery1.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery2PersonPosts.class, BaselineShortQuery2.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery3PersonFriends.class, BaselineShortQuery3.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery4MessageContent.class, BaselineShortQuery4.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery5MessageCreator.class, BaselineShortQuery5.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery6MessageForum.class, BaselineShortQuery6.class);
        OperationBindings.bindQuery(this.binder(), LdbcShortQuery7MessageReplies.class, BaselineShortQuery7.class);

        OperationBindings.bindQuery(this.binder(), LdbcQuery1.class, BaselineComplexQuery1.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery2.class, BaselineComplexQuery2.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery3.class, BaselineComplexQuery3.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery4.class, BaselineComplexQuery4.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery5.class, BaselineComplexQuery5.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery6.class, BaselineComplexQuery6.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery7.class, BaselineComplexQuery7.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery8.class, BaselineComplexQuery8.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery9.class, BaselineComplexQuery9.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery10.class, BaselineComplexQuery10.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery11.class, BaselineComplexQuery11.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery12.class, BaselineComplexQuery12.class);
        OperationBindings.bindQuery(this.binder(), LdbcQuery13.class, BaselineComplexQuery13.class);
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
