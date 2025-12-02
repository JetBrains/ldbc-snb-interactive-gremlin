package com.youtrackdb.ldbc.ytdb;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import com.jetbrains.youtrackdb.internal.core.gremlin.io.YTDBIoRegistry;
import com.jetbrains.youtrackdb.internal.driver.YTDBDriverWebSocketChannelizer;
import com.youtrackdb.ldbc.common.GraphProvider;
import org.apache.commons.lang3.function.FailableConsumer;
import org.apache.commons.lang3.function.FailableFunction;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3;
import org.apache.tinkerpop.gremlin.util.ser.AbstractMessageSerializer;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;

import java.io.IOException;
import java.util.*;

public class YtdbRemoteGraphProvider implements GraphProvider {
    private final Cluster cluster;
    private final YTDBGraphTraversalSource traversal;

    @Inject
    public YtdbRemoteGraphProvider(@Named("properties") Map<String, String> properties) {
        String host = properties.get("ytdb.host");
        Integer port = Optional.ofNullable(properties.get("ytdb.port")).map(Integer::parseInt).orElse(8182);
        String databaseName = properties.get("ytdb.database.name");
        String username = properties.get("ytdb.username");
        String password = properties.get("ytdb.password");

        var serializer = new GraphBinaryMessageSerializerV1();
        var config = new HashMap<String, Object>();
        var registries = List.of(
                YTDBIoRegistry.class.getName(),
                TinkerIoRegistryV3.class.getName()
        );

        config.put(AbstractMessageSerializer.TOKEN_IO_REGISTRIES, registries);
        serializer.configure(config, Collections.emptyMap());

        cluster = Cluster.build()
                .addContactPoint(host)
                .port(port)
                .credentials(username, password)
                .serializer(serializer)
                .channelizer(YTDBDriverWebSocketChannelizer.class)
                .create();

        final var traversalSourcePrefix = "ytdb";
        traversal = AnonymousTraversalSource
                .traversal(YTDBGraphTraversalSource.class)
                .with(DriverRemoteConnection.using(cluster, traversalSourcePrefix + databaseName));
    }

    @Override
    // Remote transactions require server-side session state, so we deliberately stay sessionless and let
    // Gremlin Server wrap every request in its own transaction. That works for this benchmark because every handler
    // issues exactly one traversal per request, so each call maps to a single auto-committed traversal.
    public <E extends Exception> void executeInTx(FailableConsumer<GraphTraversalSource, E> code) throws E {
        code.accept(traversal);
    }

    @Override
    // For reads we follow the same pattern: the server provides consistency guarantees per request and each handler
    // builds a single traversal, so we can rely on the request-scoped transaction Gremlin Server opens for us.
    public <E extends Exception, R> R computeInTx(FailableFunction<GraphTraversalSource, R, E> code) throws E {
        return code.apply(traversal);
    }

    @Override
    // The traversal source owns the underlying remote connection, so make sure we close both resources when the driver shuts down.
    public void close() throws IOException {
        try {
            traversal.close();
            cluster.close();
        } catch (Exception e) {
            throw new IOException("Error closing YTDB connection", e);
        }
    }
}
