package com.youtrackdb.ldbc.ytdb;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.youtrackdb.ldbc.common.GraphProvider;
import com.youtrackdb.ldbc.common.OperationBindings;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery1PersonProfile;
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

        bind(GraphProvider.class).to(YtdbRemoteGraphProvider.class);

        OperationBindings.bindQuery(this.binder(), LdbcShortQuery1PersonProfile.class, OptimizedShortQuery1.class);
    }
}
