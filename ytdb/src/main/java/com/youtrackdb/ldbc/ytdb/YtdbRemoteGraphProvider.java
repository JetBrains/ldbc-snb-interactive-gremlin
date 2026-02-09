package com.youtrackdb.ldbc.ytdb;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import com.jetbrains.youtrackdb.internal.core.gremlin.io.YTDBIoRegistry;
import com.jetbrains.youtrackdb.internal.driver.YTDBDriverRemoteConnection;
import com.jetbrains.youtrackdb.internal.driver.YTDBDriverWebSocketChannelizer;
import com.youtrackdb.ldbc.common.GraphProvider;
import org.apache.commons.lang3.function.FailableConsumer;
import org.apache.commons.lang3.function.FailableFunction;
import org.apache.tinkerpop.gremlin.driver.Cluster;
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
        String host = envOrProperty("YTDB_HOST", "ytdb.host", properties);
        Integer port = Optional.ofNullable(envOrProperty("YTDB_PORT", "ytdb.port", properties))
                .map(Integer::parseInt).orElse(8182);
        String database = envOrProperty("YTDB_DATABASE", "ytdb.database", properties);
        String username = envOrProperty("YTDB_USERNAME", "ytdb.username", properties);
        String password = envOrProperty("YTDB_PASSWORD", "ytdb.password", properties);

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

        var remoteConnection = new YTDBDriverRemoteConnection(cluster, false, database);
        traversal = AnonymousTraversalSource
                .traversal(YTDBGraphTraversalSource.class)
                .with(remoteConnection);
    }

    @Override
    public <E extends Exception> void executeInTx(FailableConsumer<GraphTraversalSource, E> code) throws E {
        traversal.executeInTx(code::accept);
    }

    @Override
    public <E extends Exception, R> R computeInTx(FailableFunction<GraphTraversalSource, R, E> code) throws E {
        return traversal.computeInTx(code::apply);
    }

    private static String envOrProperty(String envVar, String propKey, Map<String, String> properties) {
        String env = System.getenv(envVar);
        return env != null ? env : properties.get(propKey);
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
