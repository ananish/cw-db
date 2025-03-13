package edu.uob;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;

public class BasicDBTests {

    private DBServer server;

    @BeforeEach
    public void setup() {
        server = new DBServer();
    }

    // Helper method to send commands with a timeout.
    private String sendCommand(String command) {
        return org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(Duration.ofMillis(1000), () -> server.handleCommand(command),
                "Server took too long to respond (possibly stuck in an infinite loop)");
    }

    @Test
    public void testCreateDatabaseAndUse() {
        String dbName = "basicdb_" + System.currentTimeMillis();
        String response = sendCommand("CREATE DATABASE " + dbName + ";");
        assertTrue(response.contains("[OK]"), "Database creation should succeed.");

        response = sendCommand("USE " + dbName + ";");
        assertTrue(response.contains("[OK]"), "Switching to a valid database should succeed.");
    }

    @Test
    public void testCreateTableAndInsertSelect() {
        String dbName = "basicdb_" + System.currentTimeMillis();
        sendCommand("CREATE DATABASE " + dbName + ";");
        sendCommand("USE " + dbName + ";");
        String response = sendCommand("CREATE TABLE marks (name, mark, pass);");
        assertTrue(response.contains("[OK]"), "Table creation should succeed.");

        response = sendCommand("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        assertTrue(response.contains("[OK]"), "Record insert should succeed.");

        response = sendCommand("SELECT * FROM marks;");
        assertTrue(response.contains("[OK]"), "SELECT should return an [OK] tag.");
        assertTrue(response.contains("Simon"), "Record with 'Simon' should be present.");
    }
}
