package database;

import java.io.File;
import java.util.*;


public class Parser {
    static BST bst;
    
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
                try{
                    dbManager.useDatabase(parts[1].replace(";", ""));
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("Invalid input! Enter a valid Database name");
                }

                break;
            case "SELECT":
                if (parts.length < 4 || !parts[1].equalsIgnoreCase("*") || !parts[2].equalsIgnoreCase("FROM")) {
                    System.out.println("Syntax Error: Use SELECT * FROM <tableName>;");
                    return;
                }
                String selectTable = parts[3].replace(";", "");
                TableManager.selectAll(selectTable, dbManager.getCurrentDatabase());
                break;
                
            case "UPDATE":
                TableManager.handleUpdate(command, dbManager.getCurrentDatabase());
                break;
           
            case "INSERT":
                String tableName = "";
                Scanner scanner = new Scanner(System.in);
                tableName = parts[1];
                if(!isValidStringLength(tableName)){
                    System.out.println("Table name is too Long");
                    return;
                }
                if(dbManager.getCurrentDatabase() == null){
                    System.out.println("No Database selected.");
                    System.out.print("MiniDB > ");
                    String databaseCommand = scanner.nextLine();
                    parse(databaseCommand, dbManager);
                    parseInsertValues(command, dbManager.getCurrentDatabase(),tableName);
                    scanner.close();
                }else{
                    parseInsertValues(command, dbManager.getCurrentDatabase(),tableName);
                }
                break;
            case "DELETE":
                TableManager.handleDelete(command, dbManager.getCurrentDatabase());
                break;
            default:
                System.out.println("Unknown command: " + parts[0]);
        }
    }

    public static void parseInsertValues(String command, String dbManager, String tableName){
        int start = command.indexOf("(");
        int end = command.lastIndexOf(")");
        String stringValues = command.substring(start + 1, end).trim();
        String[] values = stringValues.split(",");//(123, "Mary")
        String path = "databases" + File.separator + dbManager.toLowerCase() + File.separator + tableName.toLowerCase() + "Attribute.txt";
        if (start == -1 || end == -1 || end < start) {
            System.out.println("Syntax Error: Attributes must be enclosed in parentheses.");
            return;
        }

        File file = new File(path);
        boolean exists = file.exists();
        if(exists){
            boolean result = TableManager.checkDataTypeInTable(tableName, values, dbManager);
            if(result){
                TableManager.writeValues(tableName, values, dbManager);
            }else{
                System.out.println("Cannot insert values into table.");
                return;
            }
        }
        else{
            System.out.println("File does not exist.");
            return;
        }

    }

    public static void parseCreateTable(String command, DatabaseManager dbManager) {
        int start = command.indexOf("(");
        int end = command.lastIndexOf(")");
        String primaryKey = null;
        
        if (start == -1 || end == -1 || end < start) {
            System.out.println("Syntax Error: Attributes must be enclosed in parentheses.");
            return;
        }
        String[] words = command.substring(2, start).trim().split("\\s+");
        String[] commandArray = command.split(" ");
        if (words.length < 3) {
            System.out.println("Syntax Error: Missing table name.");
            return;
        }
        String tableName = words[2];
        String attributesString = command.substring(start + 1, end).trim();
        String[] attributes = attributesString.split(",");
        List<String> cleanedAttributes = new ArrayList<>();
        if (command.toUpperCase().contains("PRIMARY KEY") || command.toUpperCase().contains("KEY") ){
            primaryKey = commandArray[commandArray.length-1].replace(";", "");
        }
        for (String attribute : attributes) {
            attribute = attribute.trim();
            if (attribute.toUpperCase().contains(primaryKey.toUpperCase()) ) {
                primaryKey = attribute;
                if(!isValidStringLength(primaryKey)){
                    System.out.println("Primary Key is too long.");
                    return;
                }
                cleanedAttributes.add(attribute + " primary key");
            } else {
                if(!isValidStringLength(attribute)){
                    System.out.println("Attribute name is too long.");
                    return;
                }
                cleanedAttributes.add(attribute);
            }
        }

        if (primaryKey == null || primaryKey.isEmpty()) {
            System.out.println("Primary key missing or invalid.");
        } else {
            TableManager.createTable(dbManager, tableName, cleanedAttributes.toArray(new String[0]), primaryKey);
            bst = new BST(tableName, primaryKey);
        }
    }
    public static boolean isValidStringLength(String string){
        if (string.length() > Math.pow(string.length(), 19)) {
            return false;
        }
        return true;
    }
}