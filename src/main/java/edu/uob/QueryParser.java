package edu.uob;

import java.util.*;
import java.util.regex.*;

public class QueryParser {
    public static final Set<String> KEYWORDS = Set.of(
            "USE", "CREATE", "INSERT", "SELECT", "UPDATE", "ALTER", "DELETE", "DROP", "JOIN"
    );

    public static List<String> tokenize(String query) {
        return Arrays.asList(query.trim().split("\\s+"));
    }

    public static boolean isValidQuery(String query) {
        query = query.trim().toUpperCase();
        if (!query.endsWith(";")) {
            return false;
        }
        query = query.substring(0, query.length() - 1).trim();
        List<String> tokens = tokenize(query);
        if (tokens.isEmpty() || !KEYWORDS.contains(tokens.get(0))) {
            return false;
        }
        return matchesBNFGrammar(query);
    }

    private static boolean matchesBNFGrammar(String query) {
        String usePattern = "USE\\s+[a-zA-Z0-9_]+";
        String createDatabasePattern = "CREATE\\s+DATABASE\\s+[a-zA-Z0-9_]+";
        String createTablePattern = "CREATE\\s+TABLE\\s+[a-zA-Z0-9_]+\\s*\\((\\s*[a-zA-Z0-9_]+(\\s*,\\s*[a-zA-Z0-9_]+)*\\s*)\\)";
        String insertPattern = "INSERT\\s+INTO\\s+[a-zA-Z0-9_]+\\s+VALUES\\s*\\(\\s*(('[^']*'|NULL|[0-9]+)(\\s*,\\s*('[^']*'|NULL|[0-9]+))*\\s*)\\)";
        String selectPattern = "SELECT\\s+(\\*|[a-zA-Z0-9_,\\s]+)\\s+FROM\\s+[a-zA-Z0-9_]+(\\s+WHERE\\s+.+)?";
        String updatePattern = "UPDATE\\s+[a-zA-Z0-9_]+\\s+SET\\s+[a-zA-Z0-9_]+=[^;]+\\s+WHERE\\s+.+";
        String deletePattern = "DELETE\\s+FROM\\s+[a-zA-Z0-9_]+\\s+WHERE\\s+.+";

        List<String> patterns = List.of(usePattern, createDatabasePattern, createTablePattern, insertPattern, selectPattern, updatePattern, deletePattern);
        for (String pattern : patterns) {
            if (Pattern.compile(pattern, Pattern.CASE_INSENSITIVE).matcher(query).matches()) {
                return true;
            }
        }
        return false;
    }
}
