package com.youtrackdb.ldbc.ytdb.cleanup;

import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;

import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        try (var in = Main.class.getResourceAsStream("/loader.properties")) {
            if (in != null) {
                props.load(in);
            }
        }

        String mode = System.getenv().getOrDefault("YTDB_MODE", props.getProperty("ytdb.mode", "embedded"));
        String serverHost = System.getenv().getOrDefault("YTDB_SERVER_HOST", props.getProperty("ytdb.server.host", "localhost"));
        int serverPort = Integer.parseInt(System.getenv().getOrDefault("YTDB_SERVER_PORT", props.getProperty("ytdb.server.port", "8182")));
        String serverUser = System.getenv().getOrDefault("YTDB_SERVER_USER", props.getProperty("ytdb.server.user", "root"));
        String serverPassword = System.getenv().getOrDefault("YTDB_SERVER_PASSWORD", props.getProperty("ytdb.server.password", "root"));
        String dbName = System.getenv().getOrDefault("YTDB_DATABASE_NAME", props.getProperty("ytdb.database.name"));

        System.out.println("LDBC SNB Cleanup | Mode: " + mode + " | Database: " + dbName);

        if (!"remote".equalsIgnoreCase(mode)) {
            System.err.println("Cleanup utility is only supported in remote mode.");
            System.err.println("For embedded mode, use the shell script to delete the data directory.");
            System.exit(1);
        }

        YouTrackDB db = null;

        try {
            db = YourTracks.instance(serverHost, serverPort, serverUser, serverPassword);

            if (db.exists(dbName)) {
                System.out.println("Dropping database: " + dbName);
                db.drop(dbName);
                System.out.println("Database dropped successfully.");
            } else {
                System.out.println("Database does not exist: " + dbName);
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (db != null) {
                db.close();
            }
        }
    }
}
