package com.youtrackdb.ldbc.ytdb.loader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public record LoaderConfig(
        String mode,
        String dataDir,
        String serverHost,
        int serverPort,
        String serverUser,
        String serverPassword,
        String databaseName,
        String databaseUser,
        String databasePassword,
        Path datasetPath,
        String backupPath,
        int batchSize
) {

    private static final String CLASSPATH_PROPERTIES = "/loader.properties";
    private static final int DEFAULT_BATCH_SIZE = 50_000;

    public LoaderConfig {
        if (mode == null || (!mode.equals("embedded") && !mode.equals("remote"))) {
            throw new IllegalArgumentException(
                    "ytdb.mode must be 'embedded' or 'remote', got: " + mode);
        }
        if (mode.equals("embedded") && (dataDir == null || dataDir.isBlank())) {
            throw new IllegalArgumentException(
                    "ytdb.data.dir is required in embedded mode");
        }
        if (mode.equals("remote") && (serverHost == null || serverHost.isBlank())) {
            throw new IllegalArgumentException(
                    "ytdb.server.host is required in remote mode");
        }
        if (databaseName == null || databaseName.isBlank()) {
            throw new IllegalArgumentException(
                    "ytdb.database.name is required");
        }
        if (datasetPath == null) {
            throw new IllegalArgumentException(
                    "ytdb.dataset.path is required");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException(
                    "ytdb.batch.size must be positive, got: " + batchSize);
        }
    }

    public boolean backupEnabled() {
        return backupPath != null && !backupPath.isBlank();
    }

    public void printSummary() {
        System.out.println("LDBC SNB Loader");
        System.out.println("  Mode:       " + mode);
        if (mode.equals("remote")) {
            System.out.println("  Server:     " + serverHost + ":" + serverPort);
        } else {
            System.out.println("  Data dir:   " + dataDir);
        }
        System.out.println("  Database:   " + databaseName);
        System.out.println("  Dataset:    " + datasetPath);
        System.out.println("  Batch size: " + batchSize);
        System.out.println("  Backup:     " + (backupEnabled() ? backupPath : "disabled"));
    }

    public static LoaderConfig load(String[] args) {
        Properties props = loadClasspathProperties();

        String configPath = resolveConfigPath(args);
        if (configPath != null) {
            Properties fileProps = loadFileProperties(configPath);
            props.putAll(fileProps);
        }

        String mode = env("YTDB_MODE", props, "ytdb.mode", "embedded");
        return new LoaderConfig(
                mode,
                env("YTDB_DATA_DIR", props, "ytdb.data.dir", "scratch/data"),
                env("YTDB_SERVER_HOST", props, "ytdb.server.host", "localhost"),
                parseInt(env("YTDB_SERVER_PORT", props, "ytdb.server.port", "8182")),
                env("YTDB_SERVER_USER", props, "ytdb.server.user", "root"),
                env("YTDB_SERVER_PASSWORD", props, "ytdb.server.password", "root"),
                env("YTDB_DATABASE_NAME", props, "ytdb.database.name", "ldbc_snb"),
                env("YTDB_DATABASE_USER", props, "ytdb.database.user", "admin"),
                env("YTDB_DATABASE_PASSWORD", props, "ytdb.database.password", "admin"),
                Paths.get(env("YTDB_DATASET_PATH", props, "ytdb.dataset.path", "/data")),
                env("YTDB_BACKUP_PATH", props, "ytdb.backup.path", null),
                parseInt(env("YTDB_BATCH_SIZE", props, "ytdb.batch.size",
                        String.valueOf(DEFAULT_BATCH_SIZE)))
        );
    }

    private static String resolveConfigPath(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("-c".equals(args[i]) || "--config".equals(args[i])) {
                return args[i + 1];
            }
        }
        String envPath = System.getenv("YTDB_CONFIG");
        return (envPath != null && !envPath.isBlank()) ? envPath : null;
    }

    private static String env(String envVar, Properties props, String propKey, String defaultValue) {
        String envValue = System.getenv(envVar);
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }
        return props.getProperty(propKey, defaultValue);
    }

    private static int parseInt(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value: " + value, e);
        }
    }

    private static Properties loadClasspathProperties() {
        Properties props = new Properties();
        try (InputStream in = LoaderConfig.class.getResourceAsStream(CLASSPATH_PROPERTIES)) {
            if (in != null) {
                props.load(in);
            }
        } catch (IOException e) {
            // optional
        }
        return props;
    }

    private static Properties loadFileProperties(String path) {
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(Paths.get(path))) {
            props.load(in);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Cannot read config file: " + path + " (" + e.getMessage() + ")", e);
        }
        return props;
    }
}
