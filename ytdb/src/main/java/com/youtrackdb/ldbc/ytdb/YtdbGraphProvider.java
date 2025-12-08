package com.youtrackdb.ldbc.ytdb;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import com.youtrackdb.ldbc.common.GraphProvider;
import org.apache.commons.lang3.function.FailableConsumer;
import org.apache.commons.lang3.function.FailableFunction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.IOException;
import java.util.Map;

public class YtdbGraphProvider implements GraphProvider {

    private final YouTrackDB db;
    private final YTDBGraphTraversalSource traversal;

    @Inject
    public YtdbGraphProvider(@Named("properties") Map<String, String> properties) {
        String dataDir = properties.get("ytdb.data.dir");
        String database = properties.get("ytdb.database");
        String username = properties.get("ytdb.username");
        String password = properties.get("ytdb.password");

        this.db = YourTracks.instance(dataDir);
        this.traversal = db.openTraversal(database, username, password);
    }

    @Override
    public <E extends Exception> void executeInTx(FailableConsumer<GraphTraversalSource, E> code) throws E {
        traversal.executeInTx(code::accept);
    }

    @Override
    public <E extends Exception, R> R computeInTx(FailableFunction<GraphTraversalSource, R, E> code) throws E {
        return traversal.computeInTx(code::apply);
    }

    @Override
    public void close() throws IOException {
        try {
            if (traversal != null) {
                traversal.close();
            }
            if (db != null) {
                db.close();
            }
        } catch (Exception e) {
            throw new IOException("Error closing YTDB connection", e);
        }
    }
}
