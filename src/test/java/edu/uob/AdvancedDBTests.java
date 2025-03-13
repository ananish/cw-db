package edu.uob;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;

public class AdvancedDBTests {

    private DBServer server;

    @BeforeEach
    public void setup() {
        server = new DBServer();
    }

    private String sendCommand(String command) {
        return org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(Duration.ofMillis(1000), () -> server.handleCommand(command),
                "Server took too long to respond (possibly stuck in an infinite loop)");
    }

    @Test
    public void testUpdateCommand() {
        String dbName = "advdb_" + System.currentTimeMillis();
        sendCommand("CREATE DATABASE " + dbName + ";");
        sendCommand("USE " + dbName + ";");
        sendCommand("CREATE TABLE marks (name, mark, pass);");
        sendCommand("INSERT INTO marks VALUES ('Simon', 65, TRUE);");

        String updateResponse = sendCommand("UPDATE marks SET mark = 70 WHERE name == 'Simon';");
        assertTrue(updateResponse.contains("[OK]"), "UPDATE should succeed.");

        String selectResponse = sendCommand("SELECT * FROM marks WHERE name == 'Simon';");
        assertTrue(selectResponse.contains("70"), "Updated mark should be returned.");
    }

    @Test
    public void testDeleteCommand() {
        String dbName = "advdb_" + System.currentTimeMillis();
        sendCommand("CREATE DATABASE " + dbName + ";");
        sendCommand("USE " + dbName + ";");
        sendCommand("CREATE TABLE marks (name, mark, pass);");
        sendCommand("INSERT INTO marks VALUES ('Rob', 35, FALSE);");
        sendCommand("INSERT INTO marks VALUES ('Chris', 20, FALSE);");

        String deleteResponse = sendCommand("DELETE FROM marks WHERE mark < 30;");
        assertTrue(deleteResponse.contains("[OK]"), "DELETE should succeed.");
        assertTrue(deleteResponse.contains("1 record(s) deleted"), "One record should be deleted.");

        String selectResponse = sendCommand("SELECT * FROM marks;");
        assertTrue(selectResponse.contains("Rob"), "Rob should remain.");
        assertFalse(selectResponse.contains("Chris"), "Chris should be deleted.");
    }

    @Test
    public void testAlterCommand() {
        String dbName = "advdb_" + System.currentTimeMillis();
        sendCommand("CREATE DATABASE " + dbName + ";");
        sendCommand("USE " + dbName + ";");
        sendCommand("CREATE TABLE marks (name, mark, pass);");

        // Add new column
        String addResponse = sendCommand("ALTER TABLE marks ADD grade;");
        assertTrue(addResponse.contains("[OK]"), "ALTER ADD should succeed.");

        // Drop the column
        String dropResponse = sendCommand("ALTER TABLE marks DROP grade;");
        assertTrue(dropResponse.contains("[OK]"), "ALTER DROP should succeed.");
    }

    @Test
    public void testJoinCommand() {
        String dbName = "advdb_" + System.currentTimeMillis();
        sendCommand("CREATE DATABASE " + dbName + ";");
        sendCommand("USE " + dbName + ";");
        sendCommand("CREATE TABLE table1 (name, score);");
        sendCommand("CREATE TABLE table2 (name, age);");
        sendCommand("INSERT INTO table1 VALUES ('Alice', 90);");
        sendCommand("INSERT INTO table1 VALUES ('Bob', 80);");
        sendCommand("INSERT INTO table2 VALUES ('Alice', 21);");
        sendCommand("INSERT INTO table2 VALUES ('Bob', 22);");

        String joinResponse = sendCommand("JOIN table1 AND table2 ON name AND name;");
        assertTrue(joinResponse.contains("[OK]"), "JOIN should return [OK] tag.");
        assertTrue(joinResponse.contains("table1.name"), "Joined header should include table1 columns.");
        assertTrue(joinResponse.contains("table2.age"), "Joined header should include table2 columns.");
    }

    @Test
    public void testPersistence() {

        String dbName = "advdb_" + System.currentTimeMillis();
        sendCommand("CREATE DATABASE " + dbName + ";");
        sendCommand("USE " + dbName + ";");
        sendCommand("CREATE TABLE marks (name, mark, pass);");
        sendCommand("INSERT INTO marks VALUES ('Simon', 65, TRUE);");

        // simulate server restart by creating a new DBServer object.
        server = new DBServer();
        sendCommand("USE " + dbName + ";");
        String response = sendCommand("SELECT * FROM marks;");
        assertTrue(response.contains("Simon"), "Data should persist after server restart.");
    }
}
