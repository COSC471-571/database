package database;

import java.util.*;

public class Parser {
    
    public static void parse(String command, DatabaseManager dbManager) {
        String[] parts = command.split("\\s+"); // Splitting command by spaces

        if (parts.length < 2) {
            System.out.println("Invalid command.");
            return;
        }

        switch (parts[0].toUpperCase()) {
            case "CREATE":
                if (parts[1].equalsIgnoreCase("DATABASE") && parts.length == 3) {
                    dbManager.createDatabase(parts[2]);
                } 
                else if (parts[1].equalsIgnoreCase("TABLE")) {
                    parseCreateTable(command, dbManager);
                } 
                else {
                    System.out.println("Syntax Error: Use CREATE DATABASE <Dbname>; or CREATE TABLE <tableName> (<attributes>);");
                }
                break;

            case "USE":
                if (parts.length == 2) {
                    dbManager.useDatabase(parts[1]);
                } else {
                    System.out.println("Syntax Error: Use USE <DbName>;");
                }
                break;

            default:
                System.out.println("Unknown command: " + parts[0]);
        }
    }

    private static void parseCreateTable(String command, DatabaseManager dbManager) {
        // Extracting table name and attributes
        int start = command.indexOf("(");
        int end = command.lastIndexOf(")");
        
        if (start == -1 || end == -1 || end < start) {
            System.out.println("Syntax Error: Attributes must be enclosed in parentheses.");
            return;
        }

        // Extract table name
        String[] words = command.substring(0, start).trim().split("\\s+");
        if (words.length < 3) {
            System.out.println("Syntax Error: Missing table name.");
            return;
        }
        String tableName = words[2];

        // Extract attributes inside parentheses
        String attributesString = command.substring(start + 1, end).trim();
        String[] attributes = attributesString.split(",");

        List<String> cleanedAttributes = new ArrayList<>();
        String primaryKey = null;

        for (String attribute : attributes) {
            attribute = attribute.trim();
            
            if (attribute.toUpperCase().startsWith("PRIMARY KEY")) {
                // Extract the primary key
                primaryKey = attribute.replace("PRIMARY KEY", "").trim();
                primaryKey = primaryKey.replaceAll("[()]", ""); // Remove any parentheses
            } else {
                cleanedAttributes.add(attribute);
            }
        }

        if (primaryKey == null || primaryKey.isEmpty()) {
            System.out.println("Primary key missing or invalid.");
        } else {
            // Pass cleaned attributes and primary key to database manager
            dbManager.createTable(tableName, cleanedAttributes.toArray(new String[0]), primaryKey);
        }
    }
}
