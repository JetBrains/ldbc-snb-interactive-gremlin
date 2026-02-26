package com.youtrackdb.ldbc.ytdb;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import com.youtrackdb.ldbc.common.queries.QueryTraversals;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Profiling harness for LDBC SNB Interactive Complex Read queries (IC1-IC14).
 * Delegates to {@link QueryTraversals}, reads parameters from LDBC substitution_parameters CSV files.
 *
 * <p>Uses {@link LdbcSnbDatabaseExtension} for database lifecycle management.
 *
 * <p>Env vars: {@code LDBC_PARAMS_DIR}, {@code LDBC_PARAM_ROW} (default 0),
 * {@code LDBC_QUERIES} (comma-separated filter, e.g. {@code IC3,IC5}).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("profiling")
@ExtendWith(LdbcSnbDatabaseExtension.class)
class QueryProfilerTest {

    private static final String PARAMS_RESOURCE_DIR = "/substitution_parameters";
    private static final int WARMUP_ITERATIONS = 3;

    record QueryDefinition(
            String name,
            String description,
            String paramFile,
            BiFunction<GraphTraversalSource, Map<String, String>, GraphTraversal<?, ?>> builder
    ) {}

    private static final Map<String, QueryDefinition> QUERIES = new LinkedHashMap<>();

    static {
        register("IC1", "Transitive friends with a certain name", "interactive_1_param.txt",
                (gts, params) -> QueryTraversals.ic1(gts,
                        Long.parseLong(params.get("personId")),
                        params.get("firstName"),
                        20));

        register("IC2", "Recent messages by your friends", "interactive_2_param.txt",
                (gts, params) -> QueryTraversals.ic2(gts,
                        Long.parseLong(params.get("personId")),
                        new Date(Long.parseLong(params.get("maxDate"))),
                        20));

        register("IC3", "Friends and friends of friends in given countries", "interactive_3_param.txt",
                (gts, params) -> QueryTraversals.ic3(gts,
                        Long.parseLong(params.get("personId")),
                        params.get("countryXName"),
                        params.get("countryYName"),
                        new Date(Long.parseLong(params.get("startDate"))),
                        Integer.parseInt(params.get("durationDays")),
                        20));

        register("IC4", "New topics", "interactive_4_param.txt",
                (gts, params) -> QueryTraversals.ic4(gts,
                        Long.parseLong(params.get("personId")),
                        new Date(Long.parseLong(params.get("startDate"))),
                        Integer.parseInt(params.get("durationDays")),
                        10));

        register("IC5", "New groups", "interactive_5_param.txt",
                (gts, params) -> QueryTraversals.ic5(gts,
                        Long.parseLong(params.get("personId")),
                        new Date(Long.parseLong(params.get("minDate"))),
                        20));

        register("IC6", "Tag co-occurrence", "interactive_6_param.txt",
                (gts, params) -> QueryTraversals.ic6(gts,
                        Long.parseLong(params.get("personId")),
                        params.get("tagName"),
                        10));

        register("IC7", "Recent likers", "interactive_7_param.txt",
                (gts, params) -> QueryTraversals.ic7(gts,
                        Long.parseLong(params.get("personId")),
                        20));

        register("IC8", "Recent replies", "interactive_8_param.txt",
                (gts, params) -> QueryTraversals.ic8(gts,
                        Long.parseLong(params.get("personId")),
                        20));

        register("IC9", "Recent messages by friends or friends of friends", "interactive_9_param.txt",
                (gts, params) -> QueryTraversals.ic9(gts,
                        Long.parseLong(params.get("personId")),
                        new Date(Long.parseLong(params.get("maxDate"))),
                        20));

        register("IC10", "Friend recommendation", "interactive_10_param.txt",
                (gts, params) -> QueryTraversals.ic10(gts,
                        Long.parseLong(params.get("personId")),
                        Integer.parseInt(params.get("month")),
                        10));

        register("IC11", "Job referral", "interactive_11_param.txt",
                (gts, params) -> QueryTraversals.ic11(gts,
                        Long.parseLong(params.get("personId")),
                        params.get("countryName"),
                        Integer.parseInt(params.get("workFromYear")),
                        10));

        register("IC12", "Expert search", "interactive_12_param.txt",
                (gts, params) -> QueryTraversals.ic12(gts,
                        Long.parseLong(params.get("personId")),
                        params.get("tagClassName"),
                        20));

        register("IC13", "Single shortest path", "interactive_13_param.txt",
                (gts, params) -> QueryTraversals.ic13(gts,
                        Long.parseLong(params.get("person1Id")),
                        Long.parseLong(params.get("person2Id"))));

        register("IC14", "Trusted connection paths", "interactive_14_param.txt",
                (gts, params) -> QueryTraversals.ic14(gts,
                        Long.parseLong(params.get("person1Id")),
                        Long.parseLong(params.get("person2Id"))));
    }

    private static void register(String name, String description, String paramFile,
                                 BiFunction<GraphTraversalSource, Map<String, String>, GraphTraversal<?, ?>> builder) {
        QUERIES.put(name, new QueryDefinition(name, description, paramFile, builder));
    }

    private YTDBGraphTraversalSource g;
    private Path artifactsDir;

    @BeforeAll
    void setUp(YTDBGraphTraversalSource g) throws Exception {
        this.g = g;

        String artifactsDirEnv = System.getenv("LDBC_ARTIFACTS_DIR");
        artifactsDir = artifactsDirEnv != null ? Path.of(artifactsDirEnv) : Path.of("artifacts");
        Files.createDirectories(artifactsDir);
        System.out.println("Artifacts dir: " + artifactsDir.toAbsolutePath());
    }

    private void writeArtifact(String queryName, String suffix, String content) throws IOException {
        Path file = artifactsDir.resolve(queryName.toLowerCase() + "-" + suffix + ".txt");
        Files.writeString(file, content, StandardCharsets.UTF_8);
        System.out.println("Artifact written: " + file);
    }

    @TestFactory
    Stream<DynamicTest> explainAll() {
        return filteredQueries().map(query -> DynamicTest.dynamicTest(
                "explain " + query.name() + " — " + query.description(),
                () -> {
                    var params = loadParams(query);
                    g.executeInTx(gts -> {
                        var explanation = query.builder().apply(gts, params).explain();
                        System.out.println("\n=== " + query.name() + " EXPLAIN ===");
                        System.out.println(explanation.prettyPrint());
                        writeArtifact(query.name(), "explain", explanation.prettyPrint());
                    });
                }
        ));
    }

    @TestFactory
    Stream<DynamicTest> profileAll() {
        return filteredQueries().map(query -> DynamicTest.dynamicTest(
                "profile " + query.name() + " — " + query.description(),
                () -> {
                    var params = loadParams(query);
                    g.executeInTx(gts -> {
                        System.out.println("\n=== " + query.name() + " PROFILE ===");
                        var metrics = query.builder().apply(gts, params).profile().next();
                        System.out.println(metrics);
                        writeArtifact(query.name(), "profile", metrics.toString());
                    });
                }
        ));
    }

    @TestFactory
    Stream<DynamicTest> executeAll() {
        return filteredQueries().map(query -> DynamicTest.dynamicTest(
                "execute " + query.name() + " — " + query.description(),
                () -> {
                    var params = loadParams(query);
                    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                        g.computeInTx(gts -> query.builder().apply(gts, params).toList());
                    }
                    long start = System.nanoTime();
                    var results = g.computeInTx(gts -> query.builder().apply(gts, params).toList());
                    long elapsed = System.nanoTime() - start;
                    System.out.printf("\n=== %s EXECUTE === %d results in %.1f ms%n",
                            query.name(), results.size(), elapsed / 1e6);
                    results.forEach(result -> System.out.println("  " + result));

                    StringBuilder sb = new StringBuilder();
                    sb.append(String.format("%d results in %.1f ms%n", results.size(), elapsed / 1e6));
                    results.forEach(r -> sb.append("  ").append(r).append("\n"));
                    writeArtifact(query.name(), "execute", sb.toString());
                }
        ));
    }

    private Map<String, String> loadParams(QueryDefinition query) throws IOException {
        int rowIndex = getEnvInt("LDBC_PARAM_ROW", 0);

        String paramsDir = System.getenv("LDBC_PARAMS_DIR");
        List<String> lines;
        if (paramsDir != null) {
            lines = Files.readAllLines(Path.of(paramsDir, query.paramFile()), StandardCharsets.UTF_8);
        } else {
            lines = readResourceLines(PARAMS_RESOURCE_DIR + "/" + query.paramFile());
        }

        if (lines.size() < 2) {
            throw new IllegalStateException("Param file " + query.paramFile() + " has no data rows");
        }

        String[] headers = lines.get(0).split("\\|");
        int dataLineIndex = rowIndex + 1;
        if (dataLineIndex >= lines.size()) {
            throw new IllegalStateException(
                    "Param file " + query.paramFile() + " has only " + (lines.size() - 1) +
                            " data rows, but row " + rowIndex + " was requested");
        }

        String[] values = lines.get(dataLineIndex).split("\\|");
        Map<String, String> params = new LinkedHashMap<>();
        for (int i = 0; i < headers.length && i < values.length; i++) {
            params.put(headers[i].trim(), values[i].trim());
        }
        return params;
    }

    private static List<String> readResourceLines(String resourcePath) throws IOException {
        try (var stream = QueryProfilerTest.class.getResourceAsStream(resourcePath)) {
            if (stream == null) {
                throw new IllegalStateException(
                        "Resource not found: " + resourcePath +
                                ". Set LDBC_PARAMS_DIR env var or add files to test resources.");
            }
            try (var reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                return reader.lines().collect(Collectors.toList());
            }
        }
    }

    private Stream<QueryDefinition> filteredQueries() {
        String filter = System.getenv("LDBC_QUERIES");
        if (filter == null || filter.isBlank()) {
            return QUERIES.values().stream();
        }
        Set<String> allowed = Arrays.stream(filter.split(","))
                .map(String::trim)
                .map(String::toUpperCase)
                .collect(Collectors.toSet());
        return QUERIES.values().stream()
                .filter(query -> allowed.contains(query.name().toUpperCase()));
    }

    private static int getEnvInt(String name, int defaultValue) {
        String value = System.getenv(name);
        return value != null ? Integer.parseInt(value) : defaultValue;
    }
}
