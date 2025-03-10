package edu.uob;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class FileManager {

    public static List<String> readLines(File file) {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            System.err.println("Error reading file: " + file.getName());
        }
        return lines;
    }

    public static boolean writeLines(File file, List<String> lines) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
            return true;
        } catch (IOException e) {
            System.err.println("Error writing file: " + file.getName());
            return false;
        }
    }

    public static boolean deleteFile(File file) {
        return file.delete();
    }
}
