package edu.uob;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class Database {
    private final String databaseName;
    private final String databasePath;
    private final Map<String, Table> tables;

    public Database(String name) {
        this.databaseName = name.toLowerCase();
        this.databasePath = Paths.get("databases", this.databaseName).toString();
        this.tables = new HashMap<>();
        ensureDatabaseDirectory();
        loadTables();
    }

    // New getter for the database name.
    public String getDatabaseName() {
        return databaseName;
    }

    private void ensureDatabaseDirectory() {
        try {
            Files.createDirectories(Paths.get(databasePath));
        } catch (IOException e) {
            System.err.println("Error creating database directory: " + e.getMessage());
        }
    }

    private void loadTables() {
        File folder = new File(databasePath);
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".tab"));
        if (files != null) {
            for (File file : files) {
                String tableName = file.getName().replace(".tab", "");
                tables.put(tableName.toLowerCase(), new Table(tableName, file));
            }
        }
    }

    public boolean createTable(String tableName, List<String> columns) {
        tableName = tableName.toLowerCase();
        if (tables.containsKey(tableName)) {
            return false;
        }
        Table newTable = new Table(tableName, columns, new File(databasePath, tableName + ".tab"));
        tables.put(tableName, newTable);
        return true;
    }

    public Table getTable(String tableName) {
        return tables.get(tableName.toLowerCase());
    }

    public boolean dropTable(String tableName) {
        tableName = tableName.toLowerCase();
        Table table = tables.remove(tableName);
        if (table != null) {
            return table.deleteTableFile();
        }
        return false;
    }
}
