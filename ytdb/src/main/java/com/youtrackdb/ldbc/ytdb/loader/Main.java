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

        String dbPath = System.getenv().getOrDefault("YTDB_DATA_DIR", props.getProperty("ytdb.data.dir"));
        String dbName = System.getenv().getOrDefault("YTDB_DATABASE_NAME", props.getProperty("ytdb.database.name"));
        String username = System.getenv().getOrDefault("YTDB_USERNAME", props.getProperty("ytdb.username"));
        String password = System.getenv().getOrDefault("YTDB_PASSWORD", props.getProperty("ytdb.password"));
        String datasetStr = System.getenv().getOrDefault("YTDB_TEST_DATA_DIR", props.getProperty("ytdb.dataset.path"));

        Path datasetPath = Paths.get(datasetStr);

        System.out.println("=== LDBC SNB Data Loader for YouTrackDB ===");
        System.out.println("Dataset: " + datasetPath);
        System.out.println("Database: " + dbPath + "/" + dbName);
        System.out.println();

        YouTrackDB db = null;
        YTDBGraphTraversalSource traversal = null;

        try {
            System.out.println("Connecting to YouTrackDB...");
            db = YourTracks.instance(dbPath);

            if (db.exists(dbName)) {
                System.out.println("Dropping existing database: " + dbName);
                db.drop(dbName);
            }

            System.out.println("Creating database: " + dbName);
            db.create(dbName, DatabaseType.DISK, username, password, "admin");

            traversal = db.openTraversal(dbName, username, password);
            System.out.println("Connected successfully");
            System.out.println();

            System.out.println("=== Creating Schema ===");
            SchemaCreator schemaCreator = new SchemaCreator(traversal);
            schemaCreator.createSchema();
            System.out.println("Schema created successfully");
            System.out.println();

            YtdbLoader loader = new YtdbLoader(traversal);
            System.out.println("=== Starting Data Load ===");
            long startTime = System.currentTimeMillis();

            loader.loadAll(datasetPath);

            long duration = System.currentTimeMillis() - startTime;
            System.out.println();
            System.out.println("=== Data Loading Complete ===");
            System.out.println("Total time: " + duration + "ms (" + (duration / 1000.0) + "s)");
            System.out.println();

            System.out.println("=== Entity Counts ===");
            Map<String, Long> counts = loader.counts();
            counts.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> System.out.printf("  %-20s: %,d%n", entry.getKey(), entry.getValue()));

        } catch (Exception e) {
            System.err.println("Error loading data: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            System.out.println();
            try {
                if (traversal != null) {
                    System.out.println("Closing traversal...");
                    traversal.close();
                }
            } catch (Exception e) {
                System.err.println("Error closing traversal: " + e.getMessage());
            }

            try {
                if (db != null) {
                    System.out.println("Closing database instance...");
                    db.close();
                }
            } catch (Exception e) {
                System.err.println("Error closing database: " + e.getMessage());
            }

            System.out.println("All resources closed");
        }
    }
}