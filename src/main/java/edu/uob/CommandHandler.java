package edu.uob;

import java.io.File;
import java.util.*;
import java.util.regex.*;

public class CommandHandler {
    private Database currentDatabase;

    public CommandHandler() {
        this.currentDatabase = null;
    }

    public String executeCommand(String query) {
        if (query == null || query.trim().isEmpty()) {
            return "[ERROR] Empty query";
        }
        query = query.trim();
        if (!query.endsWith(";")) {
            return "[ERROR] Query must end with a semicolon";
        }
        // Remove the trailing semicolon.
        query = query.substring(0, query.length() - 1).trim();

        List<String> tokens = QueryParser.tokenize(query);
        if (tokens.isEmpty()) {
            return "[ERROR] Empty query";
        }

        String command = tokens.get(0).toUpperCase();
        switch (command) {
            case "USE":
                return handleUse(tokens);
            case "CREATE":
                return handleCreate(query, tokens);
            case "INSERT":
                return handleInsert(query, tokens);
            case "SELECT":
                return handleSelect(query, tokens);
            case "DROP":
                return handleDrop(tokens);
            case "DELETE":
                return handleDelete(query, tokens);
            case "ALTER":
                return handleAlter(query, tokens);
            case "UPDATE":
                return handleUpdate(query, tokens);
            case "JOIN":
                return handleJoin(query, tokens);
            default:
                return "[ERROR] Unsupported command: " + command;
        }
    }

    private String handleUse(List<String> tokens) {
        if (tokens.size() < 2) return "[ERROR] Missing database name";
        String dbName = tokens.get(1).toLowerCase();
        File dbFolder = new File("databases/" + dbName);

        if (!dbFolder.exists() || !dbFolder.isDirectory()) {
            return "[ERROR] Database does not exist";
        }

        this.currentDatabase = new Database(dbName);
        return "[OK] Switched to database " + dbName;
    }

    private String handleCreate(String query, List<String> tokens) {
        if (tokens.size() < 3) {
            return "[ERROR] Invalid CREATE command";
        }
        if (tokens.get(1).equalsIgnoreCase("DATABASE")) {
            String dbName = tokens.get(2).toLowerCase();
            File dbFolder = new File("databases/" + dbName);
            if (dbFolder.exists()) {
                return "[ERROR] Database already exists";
            }
            if (dbFolder.mkdir()) {
                return "[OK] Database " + dbName + " created";
            } else {
                return "[ERROR] Failed to create database";
            }
        } else if (tokens.get(1).equalsIgnoreCase("TABLE")) {
            if (currentDatabase == null) {
                return "[ERROR] No database selected";
            }
            String tableName = tokens.get(2).toLowerCase();
            // If there is no '(' in the query, create the table with only the primary key column.
            if (!query.contains("(")) {
                List<String> columns = new ArrayList<>();
                columns.add("id");
                if (currentDatabase.createTable(tableName, columns)) {
                    return "[OK] Table " + tableName + " created";
                } else {
                    return "[ERROR] Table already exists";
                }
            }
            int start = query.indexOf('(');
            int end = query.lastIndexOf(')');
            if (start == -1 || end == -1 || end <= start) {
                return "[ERROR] Invalid CREATE TABLE syntax";
            }
            String columnString = query.substring(start + 1, end).trim();
            String[] columnArray = columnString.split(",");
            List<String> columns = new ArrayList<>();
            Set<String> columnSet = new HashSet<>();

            // Automatically add the primary key "id"
            columns.add("id");

            for (String col : columnArray) {
                String column = col.trim();
                if (column.isEmpty() || columnSet.contains(column.toLowerCase())) {
                    return "[ERROR] Invalid or duplicate column name: " + column;
                }
                columnSet.add(column.toLowerCase());
                columns.add(column);
            }
            if (currentDatabase.createTable(tableName, columns)) {
                return "[OK] Table " + tableName + " created";
            } else {
                return "[ERROR] Table already exists";
            }
        } else {
            return "[ERROR] Invalid CREATE command";
        }
    }


    private String handleInsert(String query, List<String> tokens) {
        if (tokens.size() < 4 || !tokens.get(1).equalsIgnoreCase("INTO")) {
            return "[ERROR] Invalid INSERT command";
        }
        if (currentDatabase == null) {
            return "[ERROR] No database selected";
        }
        String tableName = tokens.get(2).toLowerCase();
        Table table = currentDatabase.getTable(tableName);
        if (table == null) {
            return "[ERROR] Table not found";
        }
        int valuesPos = query.toUpperCase().indexOf("VALUES");
        if (valuesPos == -1) {
            return "[ERROR] Invalid INSERT syntax: missing VALUES";
        }
        int start = query.indexOf('(', valuesPos);
        int end = query.lastIndexOf(')');
        if (start == -1 || end == -1 || end <= start) {
            return "[ERROR] Invalid INSERT syntax: missing parentheses";
        }
        String valuesPart = query.substring(start + 1, end).trim();

        // Split values by commas while preserving strings with quotes.
        List<String> values = new ArrayList<>();
        StringBuilder currentValue = new StringBuilder();
        boolean insideQuotes = false;
        for (int i = 0; i < valuesPart.length(); i++) {
            char c = valuesPart.charAt(i);
            if (c == '\'') {
                insideQuotes = !insideQuotes;
                currentValue.append(c);
            } else if (c == ',' && !insideQuotes) {
                values.add(currentValue.toString().trim());
                currentValue.setLength(0);
            } else {
                currentValue.append(c);
            }
        }
        if (currentValue.length() > 0) {
            values.add(currentValue.toString().trim());
        }

        // Process values: if a value is a quoted string, remove the quotes.
        List<String> processedValues = new ArrayList<>();
        for (String val : values) {
            if (val.startsWith("'") && val.endsWith("'") && val.length() >= 2) {
                processedValues.add(val.substring(1, val.length() - 1));
            } else {
                processedValues.add(val);
            }
        }

        if (table.insertRow(processedValues)) {
            return "[OK] Record inserted into " + tableName;
        } else {
            return "[ERROR] Failed to insert record";
        }
    }


    private String handleSelect(String query, List<String> tokens) {
        if (currentDatabase == null) {
            return "[ERROR] No database selected";
        }
        int fromIndex = -1;
        for (int i = 0; i < tokens.size(); i++) {
            if (tokens.get(i).equalsIgnoreCase("FROM")) {
                fromIndex = i;
                break;
            }
        }
        if (fromIndex == -1 || fromIndex < 2 || tokens.size() <= fromIndex + 1) {
            return "[ERROR] Invalid SELECT syntax";
        }
        StringBuilder colSpecBuilder = new StringBuilder();
        for (int i = 1; i < fromIndex; i++) {
            colSpecBuilder.append(tokens.get(i)).append(" \n ");
        }
        String colSpec = colSpecBuilder.toString().trim();
        List<String> selectedColumns;
        if (colSpec.equals("*")) {
            String tableNameForSelect = tokens.get(fromIndex + 1).toLowerCase();
            Table table = currentDatabase.getTable(tableNameForSelect);
            if (table == null) {
                return "[ERROR] Table not found";
            }
            selectedColumns = table.getColumns();
        } else {
            selectedColumns = new ArrayList<>();
            String[] cols = colSpec.split(",");
            for (String col : cols) {
                selectedColumns.add(col.trim());
            }
        }
        String tableName = tokens.get(fromIndex + 1).toLowerCase();
        Table table = currentDatabase.getTable(tableName);
        if (table == null) {
            return "[ERROR] Table not found";
        }
        int whereIndex = -1;
        for (int i = fromIndex + 2; i < tokens.size(); i++) {
            if (tokens.get(i).equalsIgnoreCase("WHERE")) {
                whereIndex = i;
                break;
            }
        }
        List<String> results;
        if (whereIndex == -1) {
            results = table.selectRows(selectedColumns);
        } else {
            StringBuilder conditionBuilder = new StringBuilder();
            for (int i = whereIndex + 1; i < tokens.size(); i++) {
                conditionBuilder.append(tokens.get(i)).append(" \n ");
            }
            String conditionClause = conditionBuilder.toString().trim();
            if (conditionClause.startsWith("(") && conditionClause.endsWith(")")) {
                conditionClause = conditionClause.substring(1, conditionClause.length() - 1).trim();
            }
            Pattern conditionPattern = Pattern.compile("^([a-zA-Z0-9_]+)\\s*(==|!=|>=|<=|>|<|LIKE)\\s*(.+)$", Pattern.CASE_INSENSITIVE);
            Matcher matcher = conditionPattern.matcher(conditionClause);
            if (!matcher.matches()) {
                return "[ERROR] Invalid WHERE condition syntax";
            }
            String conditionAttribute = matcher.group(1);
            String comparator = matcher.group(2).toUpperCase();
            String conditionValue = matcher.group(3);
            results = table.selectRows(selectedColumns, conditionAttribute, comparator, conditionValue);
        }
        // Prepend the [OK] tag to the results.
        return "[OK] \n" + String.join("\n", results);
    }

    private String handleDrop(List<String> tokens) {
        // (Existing implementation remains unchanged.)
        if (tokens.size() < 3) {
            return "[ERROR] Invalid DROP command";
        }
        String dropType = tokens.get(1).toUpperCase();
        if (dropType.equals("DATABASE")) {
            String dbName = tokens.get(2).toLowerCase();
            File dbFolder = new File("databases/" + dbName);
            if (!dbFolder.exists() || !dbFolder.isDirectory()) {
                return "[ERROR] Database does not exist";
            }
            boolean success = deleteDirectory(dbFolder);
            if (success) {
                if (currentDatabase != null && currentDatabase.getDatabaseName().equals(dbName)) {
                    currentDatabase = null;
                }
                return "[OK] Database " + dbName + " dropped";
            } else {
                return "[ERROR] Failed to drop database " + dbName;
            }
        } else if (dropType.equals("TABLE")) {
            if (currentDatabase == null) {
                return "[ERROR] No database selected";
            }
            String tableName = tokens.get(2).toLowerCase();
            boolean success = currentDatabase.dropTable(tableName);
            if (success) {
                return "[OK] Table " + tableName + " dropped";
            } else {
                return "[ERROR] Failed to drop table " + tableName;
            }
        } else {
            return "[ERROR] Invalid DROP command";
        }
    }

    private String handleDelete(String query, List<String> tokens) {
        // (Existing implementation remains unchanged.)
        if (tokens.size() < 5 || !tokens.get(1).equalsIgnoreCase("FROM")) {
            return "[ERROR] Invalid DELETE syntax";
        }
        if (currentDatabase == null) {
            return "[ERROR] No database selected";
        }
        String tableName = tokens.get(2).toLowerCase();
        Table table = currentDatabase.getTable(tableName);
        if (table == null) {
            return "[ERROR] Table not found";
        }
        int whereIndex = -1;
        for (int i = 3; i < tokens.size(); i++) {
            if (tokens.get(i).equalsIgnoreCase("WHERE")) {
                whereIndex = i;
                break;
            }
        }
        if (whereIndex == -1) {
            return "[ERROR] DELETE requires a WHERE clause";
        }
        StringBuilder conditionBuilder = new StringBuilder();
        for (int i = whereIndex + 1; i < tokens.size(); i++) {
            conditionBuilder.append(tokens.get(i)).append(" ");
        }
        String conditionClause = conditionBuilder.toString().trim();
        if (conditionClause.startsWith("(") && conditionClause.endsWith(")")) {
            conditionClause = conditionClause.substring(1, conditionClause.length() - 1).trim();
        }
        Pattern conditionPattern = Pattern.compile("^([a-zA-Z0-9_]+)\\s*(==|!=|>=|<=|>|<|LIKE)\\s*(.+)$", Pattern.CASE_INSENSITIVE);
        Matcher matcher = conditionPattern.matcher(conditionClause);
        if (!matcher.matches()) {
            return "[ERROR] Invalid WHERE condition syntax";
        }
        String conditionAttribute = matcher.group(1);
        String comparator = matcher.group(2).toUpperCase();
        String conditionValue = matcher.group(3);
        int deletedCount = table.deleteRows(conditionAttribute, comparator, conditionValue);
        if (deletedCount < 0) {
            return "[ERROR] Column not found in WHERE clause: " + conditionAttribute;
        }
        return "[OK] " + deletedCount + " record(s) deleted from " + tableName;
    }

    private String handleAlter(String query, List<String> tokens) {
        // (Existing implementation remains unchanged.)
        if (tokens.size() != 5) {
            return "[ERROR] Invalid ALTER syntax";
        }
        if (!tokens.get(1).equalsIgnoreCase("TABLE")) {
            return "[ERROR] Invalid ALTER syntax: missing TABLE keyword";
        }
        if (currentDatabase == null) {
            return "[ERROR] No database selected";
        }
        String tableName = tokens.get(2).toLowerCase();
        Table table = currentDatabase.getTable(tableName);
        if (table == null) {
            return "[ERROR] Table not found";
        }
        String alterationType = tokens.get(3).toUpperCase();
        String attributeName = tokens.get(4);
        switch (alterationType) {
            case "ADD":
                if (table.getColumns().contains(attributeName)) {
                    return "[ERROR] Column already exists: " + attributeName;
                }
                if (table.addColumn(attributeName)) {
                    return "[OK] Column " + attributeName + " added to " + tableName;
                } else {
                    return "[ERROR] Failed to add column " + attributeName;
                }
            case "DROP":
                if (!table.getColumns().contains(attributeName)) {
                    return "[ERROR] Column not found: " + attributeName;
                }
                if (attributeName.equalsIgnoreCase("id")) {
                    return "[ERROR] Cannot drop primary key column";
                }
                if (table.dropColumn(attributeName)) {
                    return "[OK] Column " + attributeName + " dropped from " + tableName;
                } else {
                    return "[ERROR] Failed to drop column " + attributeName;
                }
            default:
                return "[ERROR] Invalid alteration type: " + alterationType;
        }
    }

    // New method to handle UPDATE command.
    private String handleUpdate(String query, List<String> tokens) {
        // Expected syntax: UPDATE <TableName> SET <NameValueList> WHERE <Condition>
        int setIndex = query.toUpperCase().indexOf(" SET ");
        if (setIndex == -1) {
            return "[ERROR] UPDATE syntax error: missing SET";
        }
        int whereIndex = query.toUpperCase().indexOf(" WHERE ");
        if (whereIndex == -1) {
            return "[ERROR] UPDATE syntax error: missing WHERE";
        }
        String tableName = tokens.get(1).toLowerCase();
        if (currentDatabase == null) {
            return "[ERROR] No database selected";
        }
        Table table = currentDatabase.getTable(tableName);
        if (table == null) {
            return "[ERROR] Table not found";
        }
        // Extract the update clause (between SET and WHERE).
        String updateClause = query.substring(setIndex + 5, whereIndex).trim();
        String[] pairs = updateClause.split(",");
        Map<String, String> updates = new HashMap<>();
        for (String pair : pairs) {
            pair = pair.trim();
            if (!pair.contains("=")) {
                return "[ERROR] Invalid SET clause, missing '=' in " + pair;
            }
            String[] keyValue = pair.split("=", 2);
            if (keyValue.length != 2) {
                return "[ERROR] Invalid SET clause in " + pair;
            }
            String key = keyValue[0].trim();
            String value = keyValue[1].trim();
            if (key.equalsIgnoreCase("id")) {
                return "[ERROR] Cannot update primary key column";
            }
            updates.put(key, value);
        }
        // Extract the condition clause (after WHERE).
        String conditionClause = query.substring(whereIndex + 7).trim();
        if (conditionClause.startsWith("(") && conditionClause.endsWith(")")) {
            conditionClause = conditionClause.substring(1, conditionClause.length() - 1).trim();
        }
        Pattern conditionPattern = Pattern.compile("^([a-zA-Z0-9_]+)\\s*(==|=|!=|>=|<=|>|<|LIKE)\\s*(.+)$", Pattern.CASE_INSENSITIVE);
        Matcher matcher = conditionPattern.matcher(conditionClause);
        if (!matcher.matches()) {
            return "[ERROR] Invalid WHERE condition syntax";
        }
        String conditionAttribute = matcher.group(1);
        String op = matcher.group(2).trim();  // Trim operator before comparison.
        String comparator = op.equals("=") ? "==" : op.toUpperCase();
        String conditionValue = matcher.group(3);
        int updateCount = table.updateRows(updates, conditionAttribute, comparator, conditionValue);
        if (updateCount < 0) {
            return "[ERROR] Column not found in WHERE clause: " + conditionAttribute;
        }
        return "[OK] " + updateCount + " record(s) updated in " + tableName;
    }

    // Helper method to recursively delete a directory.
    private boolean deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            File[] children = dir.listFiles();
            if (children != null) {
                for (File child : children) {
                    if (!deleteDirectory(child)) {
                        return false;
                    }
                }
            }
        }
        return dir.delete();
    }

    // New method: Handle JOIN command.
// Expected syntax: JOIN <TableName1> AND <TableName2> ON <AttributeName1> AND <AttributeName2>
    private String handleJoin(String query, List<String> tokens) {
        // Expect exactly 8 tokens as per the BNF.
        if (tokens.size() != 8) {
            return "[ERROR] Invalid JOIN syntax";
        }
        // tokens[0] is "JOIN", tokens[1] is table1, tokens[2] should be "AND", tokens[3] is table2,
        // tokens[4] should be "ON", tokens[5] is the join attribute from table1, tokens[6] should be "AND", tokens[7] is the join attribute from table2.
        String table1Name = tokens.get(1).toLowerCase();
        String table2Name = tokens.get(3).toLowerCase();
        String attr1 = tokens.get(5);
        String attr2 = tokens.get(7);
        if (currentDatabase == null) {
            return "[ERROR] No database selected";
        }
        Table table1 = currentDatabase.getTable(table1Name);
        Table table2 = currentDatabase.getTable(table2Name);
        if (table1 == null || table2 == null) {
            return "[ERROR] One or both tables not found";
        }
        // Retrieve column lists.
        List<String> table1Cols = table1.getColumns();
        List<String> table2Cols = table2.getColumns();
        // Check that the join attributes exist.
        int index1 = table1Cols.indexOf(attr1);
        int index2 = table2Cols.indexOf(attr2);
        if (index1 == -1) {
            return "[ERROR] Column " + attr1 + " not found in table " + table1Name;
        }
        if (index2 == -1) {
            return "[ERROR] Column " + attr2 + " not found in table " + table2Name;
        }
        // Build the joined header:
        // - A new id column.
        // - For table1, all non-id columns labeled as "table1.columnName"
        // - For table2, all non-id columns labeled as "table2.columnName"
        List<String> joinHeader = new ArrayList<>();
        joinHeader.add("id");
        for (int i = 1; i < table1Cols.size(); i++) {
            joinHeader.add(table1Name + "." + table1Cols.get(i));
        }
        for (int i = 1; i < table2Cols.size(); i++) {
            joinHeader.add(table2Name + "." + table2Cols.get(i));
        }
        List<String> joinResults = new ArrayList<>();
        joinResults.add(String.join("\t", joinHeader));
        int joinId = 1;
        // Retrieve rows from both tables.
        List<Row> rows1 = table1.getRows();
        List<Row> rows2 = table2.getRows();
        // For an inner join, for each row in table1, iterate through table2 and
        // if the join values (for the given attributes) match, create a joined row.
        for (Row r1 : rows1) {
            // Get the join value from table1. (Note: values in each table do not include the original id.)
            String joinValue1 = attr1.equalsIgnoreCase("id")
                    ? String.valueOf(r1.getId())
                    : r1.getValues().get(index1 - 1);
            for (Row r2 : rows2) {
                String joinValue2 = attr2.equalsIgnoreCase("id")
                        ? String.valueOf(r2.getId())
                        : r2.getValues().get(index2 - 1);
                if (joinValue1.equals(joinValue2)) {
                    List<String> joinedRow = new ArrayList<>();
                    joinedRow.add(String.valueOf(joinId++));
                    // Append table1's non-id values.
                    for (int i = 1; i < table1Cols.size(); i++) {
                        joinedRow.add(r1.getValues().get(i - 1));
                    }
                    // Append table2's non-id values.
                    for (int i = 1; i < table2Cols.size(); i++) {
                        joinedRow.add(r2.getValues().get(i - 1));
                    }
                    joinResults.add(String.join("\t", joinedRow));
                }
            }
        }
        return String.join("\n", joinResults);
    }

}
