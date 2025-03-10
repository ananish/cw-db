package edu.uob;

import java.io.*;
import java.util.*;

public class Table {
    private final String tableName;
    private final File tableFile;
    private final List<String> columns;
    private final List<Row> rows;
    private int nextId;

    public Table(String name, List<String> columns, File file) {
        this.tableName = name.toLowerCase();
        this.tableFile = file;
        this.columns = new ArrayList<>(columns);
        this.rows = new ArrayList<>();
        this.nextId = 1;
        saveTable();
    }

    public Table(String name, File file) {
        this.tableName = name.toLowerCase();
        this.tableFile = file;
        this.columns = new ArrayList<>();
        this.rows = new ArrayList<>();
        loadTable();
    }

    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    // SELECT without a condition.
    public List<String> selectRows(List<String> selectedColumns) {
        List<String> results = new ArrayList<>();
        results.add(String.join("\t", selectedColumns));
        for (Row row : rows) {
            List<String> selectedValues = new ArrayList<>();
            for (String col : selectedColumns) {
                int colIndex = columns.indexOf(col);
                if (colIndex == -1) {
                    return List.of("[ERROR] Column not found: " + col);
                }
                if (col.equalsIgnoreCase("id")) {
                    selectedValues.add(String.valueOf(row.getId()));
                } else {
                    selectedValues.add(row.getValues().get(colIndex - 1));
                }
            }
            results.add(String.join("\t", selectedValues));
        }
        return results;
    }

    // SELECT with a WHERE condition.
    public List<String> selectRows(List<String> selectedColumns, String conditionAttribute, String comparator, String conditionValue) {
        List<String> results = new ArrayList<>();
        results.add(String.join("\t", selectedColumns));
        int attrIndex = columns.indexOf(conditionAttribute);
        if (attrIndex == -1) {
            return List.of("[ERROR] Column not found in WHERE clause: " + conditionAttribute);
        }
        for (Row row : rows) {
            String rowValue;
            if (conditionAttribute.equalsIgnoreCase("id")) {
                rowValue = String.valueOf(row.getId());
            } else {
                rowValue = row.getValues().get(attrIndex - 1);
            }
            if (evaluateCondition(rowValue, comparator, conditionValue)) {
                List<String> selectedValues = new ArrayList<>();
                for (String col : selectedColumns) {
                    int colIndex = columns.indexOf(col);
                    if (colIndex == -1) {
                        return List.of("[ERROR] Column not found: " + col);
                    }
                    if (col.equalsIgnoreCase("id")) {
                        selectedValues.add(String.valueOf(row.getId()));
                    } else {
                        selectedValues.add(row.getValues().get(colIndex - 1));
                    }
                }
                results.add(String.join("\t", selectedValues));
            }
        }
        return results;
    }

    // New method: DELETE rows matching a condition.
    // Returns the number of rows deleted, or -1 if the condition column is not found.
    public int deleteRows(String conditionAttribute, String comparator, String conditionValue) {
        int attrIndex = columns.indexOf(conditionAttribute);
        if (attrIndex == -1) {
            return -1;
        }
        int deleteCount = 0;
        Iterator<Row> iterator = rows.iterator();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            String rowValue;
            if (conditionAttribute.equalsIgnoreCase("id")) {
                rowValue = String.valueOf(row.getId());
            } else {
                rowValue = row.getValues().get(attrIndex - 1);
            }
            if (evaluateCondition(rowValue, comparator, conditionValue)) {
                iterator.remove();
                deleteCount++;
            }
        }
        // Save changes and update nextId.
        saveTable();
        updateNextId();
        return deleteCount;
    }

    // Updated evaluateCondition method.
    // For numeric operators, if the condition value is numeric, the row value must also be numeric.
    private boolean evaluateCondition(String rowValue, String comparator, String conditionValue) {
        // Remove surrounding single quotes from conditionValue if present.
        if (conditionValue.startsWith("'") && conditionValue.endsWith("'") && conditionValue.length() >= 2) {
            conditionValue = conditionValue.substring(1, conditionValue.length() - 1);
        }
        switch (comparator) {
            case "==":
                try {
                    double condNum = Double.parseDouble(conditionValue);
                    double rowNum = Double.parseDouble(rowValue);
                    return rowNum == condNum;
                } catch (NumberFormatException e) {
                    return rowValue.equals(conditionValue);
                }
            case "!=":
                try {
                    double condNum = Double.parseDouble(conditionValue);
                    double rowNum = Double.parseDouble(rowValue);
                    return rowNum != condNum;
                } catch (NumberFormatException e) {
                    return !rowValue.equals(conditionValue);
                }
            case ">":
                try {
                    double condNum = Double.parseDouble(conditionValue);
                    double rowNum = Double.parseDouble(rowValue);
                    return rowNum > condNum;
                } catch (NumberFormatException e) {
                    return false;
                }
            case "<":
                try {
                    double condNum = Double.parseDouble(conditionValue);
                    double rowNum = Double.parseDouble(rowValue);
                    return rowNum < condNum;
                } catch (NumberFormatException e) {
                    return false;
                }
            case ">=":
                try {
                    double condNum = Double.parseDouble(conditionValue);
                    double rowNum = Double.parseDouble(rowValue);
                    return rowNum >= condNum;
                } catch (NumberFormatException e) {
                    return false;
                }
            case "<=":
                try {
                    double condNum = Double.parseDouble(conditionValue);
                    double rowNum = Double.parseDouble(rowValue);
                    return rowNum <= condNum;
                } catch (NumberFormatException e) {
                    return false;
                }
            case "LIKE":
                // Case-insensitive substring match.
                return rowValue.toLowerCase().contains(conditionValue.toLowerCase());
            default:
                return false;
        }
    }

    private void loadTable() {
        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
            String headerLine = reader.readLine();
            if (headerLine != null) {
                columns.addAll(Arrays.asList(headerLine.split("\t")));
            }
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue; // Skip blank lines.
                }
                String[] tokens = line.split("\t", -1);
                if (tokens.length == 0 || tokens[0].trim().isEmpty()) {
                    continue; // Skip lines with an empty id.
                }
                rows.add(new Row(tokens));
            }
            updateNextId();
        } catch (IOException e) {
            System.err.println("Error loading table " + tableName + ": " + e.getMessage());
        }
    }

    private void updateNextId() {
        nextId = 1;
        for (Row row : rows) {
            int rowId = row.getId();
            if (rowId >= nextId) {
                nextId = rowId + 1;
            }
        }
    }

    public boolean insertRow(List<String> values) {
        if (values.size() != columns.size() - 1) {
            return false;
        }
        Row newRow = new Row(nextId++, values);
        rows.add(newRow);
        return saveTable();
    }

    private boolean saveTable() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile))) {
            writer.write(String.join("\t", columns));
            writer.newLine();
            for (Row row : rows) {
                writer.write(row.toString());
                writer.newLine();
            }
            return true;
        } catch (IOException e) {
            System.err.println("Error saving table " + tableName + ": " + e.getMessage());
            return false;
        }
    }

    public boolean addColumn(String attributeName) {
        if (attributeName.equalsIgnoreCase("id")) {
            return false; // Cannot add a column named "id".
        }
        if (columns.contains(attributeName)) {
            return false;
        }
        columns.add(attributeName);
        // For each row, add a default empty string for the new column.
        for (Row row : rows) {
            row.getValues().add("");
        }
        return saveTable();
    }

    public boolean dropColumn(String attributeName) {
        if (attributeName.equalsIgnoreCase("id")) {
            return false; // Cannot drop the primary key column.
        }
        int index = columns.indexOf(attributeName);
        if (index == -1) {
            return false;
        }
        columns.remove(index);
        // Remove the corresponding value from each row.
        for (Row row : rows) {
            if (index - 1 >= 0 && index - 1 < row.getValues().size()) {
                row.getValues().remove(index - 1);
            }
        }
        return saveTable();
    }

    public boolean deleteTableFile() {
        return tableFile.delete();
    }

    public int updateRows(Map<String, String> updates, String conditionAttribute, String comparator, String conditionValue) {
        int condIndex = columns.indexOf(conditionAttribute);
        if (condIndex == -1) {
            return -1;
        }
        int updateCount = 0;
        for (Row row : rows) {
            String rowValue;
            if (conditionAttribute.equalsIgnoreCase("id")) {
                rowValue = String.valueOf(row.getId());
            } else {
                rowValue = row.getValues().get(condIndex - 1);
            }
            if (evaluateCondition(rowValue, comparator, conditionValue)) {
                // For each update, update the corresponding column value.
                for (Map.Entry<String, String> entry : updates.entrySet()) {
                    String colName = entry.getKey();
                    String newValue = entry.getValue();
                    if (colName.equalsIgnoreCase("id")) {
                        continue; // Skip updating primary key.
                    }
                    int colIndex = columns.indexOf(colName);
                    if (colIndex == -1) {
                        return -1; // Column not found.
                    }
                    // Row values correspond to columns starting from index 1 (since "id" is at columns[0]).
                    row.getValues().set(colIndex - 1, newValue);
                }
                updateCount++;
            }
        }
        saveTable();
        return updateCount;
    }

    // New method: Return a copy of the current rows.
    public List<Row> getRows() {
        return new ArrayList<>(rows);
    }

}