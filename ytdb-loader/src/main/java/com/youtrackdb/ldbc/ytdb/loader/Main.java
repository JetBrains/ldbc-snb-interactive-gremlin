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

        // Connection configuration
        String mode = getConfig("YTDB_MODE", props, "ytdb.mode", "embedded");
        String dbPath = getConfig("YTDB_DATA_DIR", props, "ytdb.data.dir", null);
        String serverHost = getConfig("YTDB_SERVER_HOST", props, "ytdb.server.host", "localhost");
        int serverPort = Integer.parseInt(getConfig("YTDB_SERVER_PORT", props, "ytdb.server.port", "8182"));
        String serverUser = getConfig("YTDB_SERVER_USER", props, "ytdb.server.user", "root");
        String serverPassword = getConfig("YTDB_SERVER_PASSWORD", props, "ytdb.server.password", "root");
        String dbName = getConfig("YTDB_DATABASE_NAME", props, "ytdb.database.name", null);
        String databaseUser = getConfig("YTDB_DATABASE_USER", props, "ytdb.database.user", null);
        String databasePassword = getConfig("YTDB_DATABASE_PASSWORD", props, "ytdb.database.password", null);
        // Dataset path - where the LDBC CSV files are located (client-side)
        Path datasetPath = Paths.get(getConfig("YTDB_DATASET_PATH", props, "ytdb.dataset.path", null));

        // Backup path (optional, server-side path where backups are stored)
        String backupPath = getConfig("YTDB_BACKUP_PATH", props, "ytdb.backup.path", null);
        boolean backupEnabled = backupPath != null && !backupPath.isBlank();

        System.out.println("LDBC SNB Loader");
        System.out.println("  Mode:       " + mode);
        System.out.println("  Dataset:    " + datasetPath);
        System.out.println("  Backup:     " + (backupEnabled ? backupPath : "disabled"));

        YouTrackDB db = null;
        YTDBGraphTraversalSource traversal = null;

        try {
            db = "remote".equalsIgnoreCase(mode)
                    ? YourTracks.instance(serverHost, serverPort, serverUser, serverPassword)
                    : YourTracks.instance(dbPath);

            long startTime = System.currentTimeMillis();
            boolean restored = false;

            if (backupEnabled) {
                restored = tryRestore(db, dbName, backupPath);
            }

            if (restored) {
                traversal = db.openTraversal(dbName);
            } else {
                // Load from CSVs
                System.out.println("\nLoading data from CSVs...");
                dropIfExists(db, dbName);
                db.create(dbName, DatabaseType.DISK, databaseUser, databasePassword, "admin");
                traversal = db.openTraversal(dbName, databaseUser, databasePassword);

                new SchemaCreator(traversal).createSchema();

                YtdbLoader loader = new YtdbLoader(traversal);
                loader.loadAll(datasetPath);

                System.out.println("\nLoaded entities:");
                Map<String, Long> counts = loader.counts();
                counts.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .forEach(entry -> System.out.printf("  %-20s: %,d%n", entry.getKey(), entry.getValue()));

                if (backupEnabled) {
                    System.out.println("\nCreating backup at " + backupPath + "...");
                    String backupFile = traversal.backup(Paths.get(backupPath));
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
            System.out.println("Falling back to loading from CSVs.");
            // Clean up any partially created database from failed restore
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

    private static String getConfig(String envVar, Properties props, String propKey, String defaultValue) {
        String envValue = System.getenv(envVar);
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }
        return props.getProperty(propKey, defaultValue);
    }
}
