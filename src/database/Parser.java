package database;

import java.util.*;


public class Parser {
    
    public static void parse(String command, DatabaseManager dbManager) {
        String[] parts = command.split("\\s+"); 
        

        if (parts.length < 2) {
            System.out.println("Invalid command.");
            return;
        }

        switch (parts[0].toUpperCase()) {
            case "CREATE":
                if (parts[1].equalsIgnoreCase("DATABASE")) {
                    dbManager.createDatabase(parts[2].replace(";", ""));
                } else if (parts[1].equalsIgnoreCase("TABLE")) {
                    parseCreateTable(command, dbManager);
                } else {
                    System.out.println("Syntax Error: Use CREATE DATABASE <Dbname>; or CREATE TABLE <tableName> (<attributes>);");
                }
                break;
            case "USE":
                dbManager.useDatabase(parts[2].replace(";", ""));
                break;
            case "SELECT":
                break;
            case "INSERT":
                String tableName = "";
                dbManager.useDatabase(parts[2]);
                if(dbManager.isCreated() != true){
                    dbManager.createDatabase(parts[2].replace(";", ""));
                }else{
                    tableName = parts[2];
                }
                if(parts[3].equalsIgnoreCase("VALUES")){
                    parseCreateValues(command, dbManager, tableName);
                }
                break;
            default:
                System.out.println("Unknown command: " + parts[0]);
        }
    }
    //continue here
    public static void parseCreateValues(String command, DatabaseManager dbManager, String tableName){
        String primaryKey = "";
        int start = command.indexOf("(");
        int end = command.lastIndexOf(")");

        dbManager.useDatabase(tableName);

        if (start == -1 || end == -1 || end < start) {
            System.out.println("Syntax Error: Attributes must be enclosed in parentheses.");
            return;
        }
        String stringValues = command.substring(start + 1, end).trim();
        String[] dataType = stringValues.split(",");//(123, "Mary")


        if(TableManager.checkDataTypeInTable(tableName, dataType)){ //if true
            //execute code in here call write to records file method and write to the file.
        }else{
            System.out.println("Inserted values do not match table datatypes.");
            return;
        }


    }

    public static void parseCreateTable(String command, DatabaseManager dbManager) {
        int start = command.indexOf("(");
        int end = command.lastIndexOf(")");
        
        if (start == -1 || end == -1 || end < start) {
            System.out.println("Syntax Error: Attributes must be enclosed in parentheses.");
            return;
        }


        String[] words = command.substring(2, start).trim().split("\\s+");

//        if (words.length < 3) {
//            System.out.println("Syntax Error: Missing table name.");
//            return;
//        }
        String tableName = words[2];

        String attributesString = command.substring(start + 1, end).trim();
        String[] attributes = attributesString.split(",");

        List<String> cleanedAttributes = new ArrayList<>();
        String primaryKey = null;

        for (String attribute : attributes) {
            attribute = attribute.trim();
            if (attribute.toUpperCase().contains("PRIMARY KEY")) {
                primaryKey = attribute.toUpperCase().replace("PRIMARY KEY", "").trim();
                primaryKey = primaryKey.toLowerCase().replaceAll("[()]", "");
                cleanedAttributes.add(primaryKey);
            } else {
                cleanedAttributes.add(attribute);
            }
        }

        if (primaryKey == null || primaryKey.isEmpty()) {
            System.out.println("Primary key missing or invalid.");
        } else {
            TableManager.createTable(dbManager, tableName, cleanedAttributes.toArray(new String[0]), primaryKey);
        }
    }

}
