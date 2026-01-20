package com.youtrackdb.ldbc.ytdb.loader;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        try (var in = Main.class.getResourceAsStream("/loader.properties")) {
            props.load(in);
        }

        String mode = System.getenv().getOrDefault("YTDB_MODE", props.getProperty("ytdb.mode", "embedded"));
        String dbPath = System.getenv().getOrDefault("YTDB_DATA_DIR", props.getProperty("ytdb.data.dir"));
        String serverHost = System.getenv().getOrDefault("YTDB_SERVER_HOST", props.getProperty("ytdb.server.host", "localhost"));
        int serverPort = Integer.parseInt(System.getenv().getOrDefault("YTDB_SERVER_PORT", props.getProperty("ytdb.server.port", "8182")));
        String serverUser = System.getenv().getOrDefault("YTDB_SERVER_USER", props.getProperty("ytdb.server.user", "root"));
        String serverPassword = System.getenv().getOrDefault("YTDB_SERVER_PASSWORD", props.getProperty("ytdb.server.password", "root"));
        String dbName = System.getenv().getOrDefault("YTDB_DATABASE_NAME", props.getProperty("ytdb.database.name"));
        String databaseUser = System.getenv().getOrDefault("YTDB_DATABASE_USER", props.getProperty("ytdb.database.user"));
        String databasePassword = System.getenv().getOrDefault("YTDB_DATABASE_PASSWORD", props.getProperty("ytdb.database.password"));
        Path datasetPath = Paths.get(System.getenv().getOrDefault("YTDB_TEST_DATA_DIR", props.getProperty("ytdb.dataset.path")));

        System.out.println("LDBC SNB Loader | Mode: " + mode + " | Dataset: " + datasetPath);

        YouTrackDB db = null;
        YTDBGraphTraversalSource traversal = null;

        try {
            db = "remote".equalsIgnoreCase(mode)
                    ? YourTracks.instance(serverHost, serverPort, serverUser, serverPassword)
                    : YourTracks.instance(dbPath);

            if (db.exists(dbName)) {
                db.drop(dbName);
            }

            db.create(dbName, DatabaseType.DISK, databaseUser, databasePassword, "admin");
            traversal = db.openTraversal(dbName, databaseUser, databasePassword);

            new SchemaCreator(traversal).createSchema();

            long startTime = System.currentTimeMillis();
            YtdbLoader loader = new YtdbLoader(traversal);
            loader.loadAll(datasetPath);

            long duration = System.currentTimeMillis() - startTime;
            System.out.println("\nCompleted in " + duration + "ms (" + (duration / 1000.0) + "s)\n");

            Map<String, Long> counts = loader.counts();
            counts.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> System.out.printf("  %-20s: %,d%n", entry.getKey(), entry.getValue()));

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (traversal != null) {
                traversal.close();
            }

            if (db != null) {
                db.close();
            }
        }
    }
}
