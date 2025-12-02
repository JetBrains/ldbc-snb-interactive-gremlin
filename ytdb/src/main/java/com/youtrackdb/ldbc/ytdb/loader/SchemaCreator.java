package com.youtrackdb.ldbc.ytdb.loader;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class SchemaCreator {

    private static final Logger log = LoggerFactory.getLogger(SchemaCreator.class);
    private static final String SCHEMA_FILE = "/ldbc-snb-schema.osql";

    private final YTDBGraph graph;

    public SchemaCreator(YTDBGraph graph) {
        this.graph = graph;
    }

    public void createSchema() {
        log.info("Creating LDBC SNB schema...");
        long startTime = System.currentTimeMillis();

        try {
            List<String> statements = loadSchemaStatements();
            log.info("Loaded {} SQL statements from schema file", statements.size());

            // Execute all statements in a single transaction
            graph.executeInTx(g -> {
                for (String statement : statements) {
                    g.command(statement);
                }
            });

            long duration = System.currentTimeMillis() - startTime;
            log.info("Schema creation completed successfully in {}ms ({} statements)",
                     duration, statements.size());

        } catch (Exception e) {
            log.error("Failed to create schema", e);
            throw new RuntimeException("Schema creation failed", e);
        }
    }

    private List<String> loadSchemaStatements() throws IOException {
        List<String> statements = new ArrayList<>();
        StringBuilder currentStatement = new StringBuilder();

        try (InputStream is = getClass().getResourceAsStream(SCHEMA_FILE)) {
            if (is == null) {
                throw new IOException("Schema file not found: " + SCHEMA_FILE);
            }

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(is, StandardCharsets.UTF_8))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();

                    // Skip empty lines and comments
                    if (line.isEmpty() || line.startsWith("--")) {
                        continue;
                    }

                    // Skip SET commands (they're not executable in this context)
                    if (line.toUpperCase().startsWith("SET ")) {
                        continue;
                    }

                    // Accumulate statement until semicolon
                    currentStatement.append(line);

                    if (line.endsWith(";")) {
                        // Remove trailing semicolon and add complete statement
                        String statement = currentStatement.toString();
                        statement = statement.substring(0, statement.length() - 1).trim();

                        if (!statement.isEmpty()) {
                            statements.add(statement);
                        }

                        currentStatement.setLength(0); // Reset for next statement
                    } else {
                        // Multi-line statement - add space between lines
                        currentStatement.append(" ");
                    }
                }

                // Handle any remaining statement without semicolon
                if (currentStatement.length() > 0) {
                    String statement = currentStatement.toString().trim();
                    if (!statement.isEmpty()) {
                        statements.add(statement);
                    }
                }
            }
        }

        return statements;
    }
}
