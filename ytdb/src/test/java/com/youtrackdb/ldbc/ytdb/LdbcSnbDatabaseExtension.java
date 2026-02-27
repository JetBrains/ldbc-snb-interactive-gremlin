package com.youtrackdb.ldbc.ytdb;

import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

import static com.youtrackdb.ldbc.common.LdbcSchema.PERSON;

/**
 * JUnit extension that initializes a shared LDBC SNB database once per test run.
 *
 * <p>The fixture is stored in the root {@link ExtensionContext.Store} and cleaned up
 * automatically when the root context is closed.
 */
final class LdbcSnbDatabaseExtension implements BeforeAllCallback, ParameterResolver {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(LdbcSnbDatabaseExtension.class);
  private static final String BACKUP_RESOURCE_DIR = "/backups";
  private static final String DATABASE_NAME = "ldbc_snb";
  private static final String WORK_DIR_NAME = "test-data";

  @Override
  public void beforeAll(ExtensionContext context) {
    getFixture(context);
  }

  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
    Class<?> type = parameterContext.getParameter().getType();
    return type == YTDBGraphTraversalSource.class || type == YouTrackDB.class;
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
    Fixture fixture = getFixture(extensionContext);
    Class<?> type = parameterContext.getParameter().getType();
    if (type == YTDBGraphTraversalSource.class) {
      return fixture.traversal();
    }
    if (type == YouTrackDB.class) {
      return fixture.db();
    }
    throw new ParameterResolutionException("Unsupported parameter type: " + type.getName());
  }

  private static Fixture getFixture(ExtensionContext context) {
    ExtensionContext.Store store = context.getRoot().getStore(NAMESPACE);
    return store.getOrComputeIfAbsent(Fixture.class, ignored -> new Fixture(), Fixture.class);
  }

  static final class Fixture implements AutoCloseable {
    private final YouTrackDB db;
    private final YTDBGraphTraversalSource traversal;
    private final Path workDir;

    Fixture() {
      try {
        workDir = resolveWorkDir();
        Path backupDir = workDir.resolve("backup");
        Path dataDir = workDir.resolve("databases");
        Files.createDirectories(dataDir);

        if (!Files.isDirectory(dataDir.resolve(DATABASE_NAME))) {
          Files.createDirectories(backupDir);
          boolean backupEmpty;
          try (Stream<Path> walk = Files.list(backupDir)) {
            backupEmpty = walk.findAny().isEmpty();
          }
          if (backupEmpty) {
            copyResourceDirectory(BACKUP_RESOURCE_DIR, backupDir);
          }
          db = YourTracks.instance(dataDir.toString());
          db.restore(DATABASE_NAME, backupDir.toString());
        } else {
          db = YourTracks.instance(dataDir.toString());
        }

        traversal = db.openTraversal(DATABASE_NAME, "admin", "admin");
        traversal.computeInTx(g -> g.V().hasLabel(PERSON).count().next());
      } catch (Exception e) {
        throw new RuntimeException("Failed to initialize LDBC SNB fixture", e);
      }
    }

    YTDBGraphTraversalSource traversal() {
      return traversal;
    }

    YouTrackDB db() {
      return db;
    }

    @Override
    public void close() {
      try {
        traversal.close();
      } catch (Exception e) {
        // best-effort cleanup
      }
      try {
        db.close();
      } catch (Exception e) {
        // best-effort cleanup
      }
    }
  }

  private static Path resolveWorkDir() throws Exception {
    Path base = Path.of(
        LdbcSnbDatabaseExtension.class.getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .toURI());
    Path cursor = base;
    while (cursor != null && !Files.exists(cursor.resolve("pom.xml"))) {
      cursor = cursor.getParent();
    }
    if (cursor == null) {
      throw new IllegalStateException("Unable to locate module root for test-data directory");
    }
    Path workDir = cursor.resolve(WORK_DIR_NAME);
    Files.createDirectories(workDir);
    return workDir;
  }

  private static void copyResourceDirectory(String resourceDir, Path target) throws Exception {
    var url = LdbcSnbDatabaseExtension.class.getResource(resourceDir);
    if (url == null) {
      throw new IllegalStateException("Backup resources not found at classpath:" + resourceDir);
    }
    Path source = Path.of(url.toURI());
    Files.createDirectories(target);
    try (Stream<Path> walk = Files.walk(source)) {
      walk.forEach(src -> {
        Path dest = target.resolve(source.relativize(src));
        try {
          if (Files.isDirectory(src)) {
            Files.createDirectories(dest);
          } else {
            Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
          }
        } catch (IOException e) {
          throw new RuntimeException("Failed to copy " + src + " to " + dest, e);
        }
      });
    }
  }

}
