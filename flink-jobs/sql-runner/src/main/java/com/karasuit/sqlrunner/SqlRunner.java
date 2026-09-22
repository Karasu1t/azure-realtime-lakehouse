package com.karasuit.sqlrunner;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Reads every *.sql file in a directory, in filename order, and runs each
 * ';'-separated statement through a single TableEnvironment session --
 * this is what turns flink-jobs/inventory-monitor/01_catalog.sql through
 * 04_pipeline.sql into one running Flink job. Mirrors the "SQL runner"
 * pattern from Apache Flink's own flink-kubernetes-operator examples.
 *
 * The final INSERT INTO statement is the only one that submits a
 * long-running job; everything before it (CREATE CATALOG/TABLE/DATABASE)
 * completes immediately, which is why those must come first in filename
 * order.
 */
public final class SqlRunner {

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: SqlRunner <sql-directory>");
        }

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        for (Path sqlFile : sortedSqlFiles(Path.of(args[0]))) {
            for (String statement : splitStatements(Files.readString(sqlFile, StandardCharsets.UTF_8))) {
                System.out.println("[" + sqlFile.getFileName() + "] executing: " + firstLine(statement));
                tEnv.executeSql(statement);
            }
        }
    }

    private static List<Path> sortedSqlFiles(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            List<Path> sqlFiles = files
                .filter(p -> p.toString().endsWith(".sql"))
                .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                .toList();
            return new ArrayList<>(sqlFiles);
        }
    }

    // Splits on ';' but not inside a single-quoted string literal --
    // Event Hubs connection strings are themselves ';'-delimited
    // (Endpoint=...;SharedAccessKeyName=...;SharedAccessKey=...), so a
    // naive split broke mid-value once a real connection string (rather
    // than the ${VAR} placeholder) was substituted in.
    private static List<String> splitStatements(String fileContents) {
        String withoutComments = fileContents.lines()
            .filter(line -> !line.strip().startsWith("--"))
            .reduce((a, b) -> a + "\n" + b)
            .orElse("");

        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inString = false;
        for (int i = 0; i < withoutComments.length(); i++) {
            char c = withoutComments.charAt(i);
            if (c == '\'') {
                inString = !inString;
                current.append(c);
            } else if (c == ';' && !inString) {
                addIfNotBlank(statements, current);
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        addIfNotBlank(statements, current);
        return statements;
    }

    private static void addIfNotBlank(List<String> statements, StringBuilder current) {
        String stmt = current.toString().strip();
        if (!stmt.isEmpty()) {
            statements.add(stmt);
        }
    }

    private static String firstLine(String statement) {
        int idx = statement.indexOf('\n');
        return idx == -1 ? statement : statement.substring(0, idx);
    }
}
