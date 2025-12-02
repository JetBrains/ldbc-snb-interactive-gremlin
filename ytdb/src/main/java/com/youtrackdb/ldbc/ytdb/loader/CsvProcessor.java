package com.youtrackdb.ldbc.ytdb.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public final class CsvProcessor<T> {

    private static final Logger log = LoggerFactory.getLogger(CsvProcessor.class);
    private static final String DELIMITER = "\\|";

    private final int batchSize;

    public CsvProcessor(int batchSize) {
        this.batchSize = batchSize;
    }

    public long process(Path csvFile,
                       Function<String[], T> parser,
                       Consumer<List<T>> batchConsumer) throws IOException {

        log.debug("Processing CSV file: {}", csvFile.getFileName());

        try (Stream<String> lines = Files.lines(csvFile)) {
            var batch = new ArrayList<T>(batchSize);
            long count = 0;

            // Skip header and process lines
            var iterator = lines.skip(1).iterator();

            while (iterator.hasNext()) {
                String line = iterator.next();
                String[] fields = line.split(DELIMITER, -1);

                try {
                    T record = parser.apply(fields);
                    batch.add(record);
                    count++;

                    if (batch.size() >= batchSize) {
                        batchConsumer.accept(List.copyOf(batch));
                        batch.clear();
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse line in {}: {}", csvFile.getFileName(), e.getMessage());
                }
            }

            if (!batch.isEmpty()) {
                batchConsumer.accept(List.copyOf(batch));
            }

            return count;
        }
    }

    public static String parseString(String value) {
        return value.isEmpty() ? null : value;
    }

    public static List<String> parseList(String value) {
        if (value.isEmpty()) {
            return List.of();
        }
        return List.of(value.split(";"));
    }
}
