package com.youtrackdb.ldbc.ytdb.loader;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

import java.nio.file.Paths;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws Exception {
        LoaderConfig config = LoaderConfig.load(args);
        config.printSummary();

        YouTrackDB db = null;
        YTDBGraphTraversalSource traversal = null;

        try {
            db = "remote".equalsIgnoreCase(config.mode())
                    ? YourTracks.instance(config.serverHost(), config.serverPort(),
                            config.serverUser(), config.serverPassword())
                    : YourTracks.instance(config.dataDir());

            long startTime = System.currentTimeMillis();
            boolean restored = false;

            if (config.backupEnabled()) {
                restored = tryRestore(db, config.databaseName(), config.backupPath());
            }

            if (restored) {
                traversal = "remote".equalsIgnoreCase(config.mode())
                        ? db.openTraversal(config.databaseName())
                        : db.openTraversal(config.databaseName(),
                                config.databaseUser(), config.databasePassword());
            } else {
                System.out.println("\nLoading data from CSVs...");
                dropIfExists(db, config.databaseName());
                db.create(config.databaseName(), DatabaseType.DISK,
                        config.databaseUser(), config.databasePassword(), "admin");
                traversal = db.openTraversal(config.databaseName(),
                        config.databaseUser(), config.databasePassword());

                new SchemaCreator(traversal).createSchema();

                YtdbLoader loader = new YtdbLoader(traversal, config.batchSize());
                loader.loadAll(config.datasetPath());

                System.out.println("\nLoaded entities:");
                Map<String, Long> counts = loader.counts();
                counts.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .forEach(entry -> System.out.printf("  %-20s: %,d%n",
                                entry.getKey(), entry.getValue()));

                if (config.backupEnabled()) {
                    System.out.println("\nCreating backup at " + config.backupPath() + "...");
                    String backupFile = traversal.backup(Paths.get(config.backupPath()));
                    System.out.println("Backup created: " + backupFile);
                }
            }

            long duration = System.currentTimeMillis() - startTime;
            System.out.println("\nCompleted in " + duration + "ms (" + (duration / 1000.0) + "s)");

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

    private static boolean tryRestore(YouTrackDB db, String dbName, String backupPath) {
        System.out.println("\nAttempting to restore from backup at " + backupPath + "...");
        try {
            dropIfExists(db, dbName);
            db.restore(dbName, backupPath);
            System.out.println("Database restored successfully from backup.");
            return true;
        } catch (Exception e) {
            System.out.println("No backup found or restore failed: " + e.getMessage());
            e.printStackTrace(System.out);
            System.out.println("Falling back to loading from CSVs.");
            dropIfExists(db, dbName);
            return false;
        }
    }

    private static void dropIfExists(YouTrackDB db, String dbName) {
        try {
            db.drop(dbName);
            System.out.println("Dropped existing database: " + dbName);
        } catch (Exception e) {
            // DB doesn't exist
        }
    }
}
