package com.youtrackdb.ldbc.runner;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import com.youtrackdb.ldbc.common.GraphProvider;
import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import com.youtrackdb.ldbc.common.DefaultQueryModule;
import com.youtrackdb.ldbc.ytdb.YtdbModule;
import org.ldbcouncil.snb.driver.Db;
import org.ldbcouncil.snb.driver.DbConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.control.LoggingService;
import org.ldbcouncil.snb.driver.workloads.interactive.*;

import java.io.IOException;
import java.util.Map;

public class TinkerPopDb extends Db {
    private TinkerPopConnectionState connectionState;
    private Injector injector;
    private LoggingService loggingService;

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        this.loggingService = loggingService;
        loggingService.info("Initializing TinkerPop LDBC SNB implementation");

        String vendor = properties.get("tinkerpop.vendor");
        loggingService.info("Selected vendor: " + vendor);

        Module vendorModule = createVendorModule(vendor, properties);
        this.injector = Guice.createInjector(
            Modules.override(new DefaultQueryModule())
                   .with(vendorModule)
        );

        GraphProvider graphProvider = injector.getInstance(GraphProvider.class);
        this.connectionState = new TinkerPopConnectionState(graphProvider, properties);

        registerAllOperationHandlers();

        loggingService.info("TinkerPop LDBC SNB initialization complete");
    }

    private Module createVendorModule(String vendor, Map<String, String> properties) throws DbException {
        return switch (vendor.toLowerCase()) {
            case "ytdb" -> new YtdbModule(properties);
            default -> throw new DbException("Unknown vendor: " + vendor + ". Supported: ytdb");
        };
    }

    private void registerAllOperationHandlers() throws DbException {
        loggingService.info("Registering operation handlers...");

        // Register Short Queries (IS1-IS7)
        registerHandler(LdbcShortQuery1PersonProfile.class);
        registerHandler(LdbcShortQuery2PersonPosts.class);
        registerHandler(LdbcShortQuery3PersonFriends.class);
        registerHandler(LdbcShortQuery4MessageContent.class);
        registerHandler(LdbcShortQuery5MessageCreator.class);
        registerHandler(LdbcShortQuery6MessageForum.class);
        registerHandler(LdbcShortQuery7MessageReplies.class);

        // Register Complex Queries (IC1-IC14)
        registerHandler(LdbcQuery1.class);
        registerHandler(LdbcQuery2.class);
        registerHandler(LdbcQuery3.class);
        registerHandler(LdbcQuery4.class);
        registerHandler(LdbcQuery5.class);
        registerHandler(LdbcQuery6.class);
        registerHandler(LdbcQuery7.class);
        registerHandler(LdbcQuery8.class);
        registerHandler(LdbcQuery9.class);
        registerHandler(LdbcQuery10.class);
        registerHandler(LdbcQuery11.class);
        registerHandler(LdbcQuery12.class);
        registerHandler(LdbcQuery13.class);
        registerHandler(LdbcQuery14.class);

        // Register Update Operations (INS1-INS8)
        registerHandler(LdbcUpdate1AddPerson.class);
        registerHandler(LdbcUpdate2AddPostLike.class);
        registerHandler(LdbcUpdate3AddCommentLike.class);
        registerHandler(LdbcUpdate4AddForum.class);
        registerHandler(LdbcUpdate5AddForumMembership.class);
        registerHandler(LdbcUpdate6AddPost.class);
        registerHandler(LdbcUpdate7AddComment.class);
        registerHandler(LdbcUpdate8AddFriendship.class);

        loggingService.info("Operation handler registration complete");
    }

    private <T extends Operation<?>> void registerHandler(Class<T> operationType)
            throws DbException {
        try {
            String queryName = operationType.getSimpleName();

            @SuppressWarnings("unchecked")
            OperationHandler<T, TinkerPopConnectionState> handler =
                (OperationHandler<T, TinkerPopConnectionState>)
                    injector.getInstance(Key.get(OperationHandler.class, Names.named(queryName)));

            //noinspection unchecked
            registerOperationHandler(operationType, handler.getClass());

        } catch (Exception e) {
            throw new DbException("Failed to register handler for " +
                                operationType.getSimpleName(), e);
        }
    }

    @Override
    protected void onClose() throws IOException {
        if (connectionState != null) {
            connectionState.close();
        }
        if (injector != null) {
            injector.getInstance(GraphProvider.class).close();
        }
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return connectionState;
    }
}
