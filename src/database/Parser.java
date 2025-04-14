package database;

import java.io.File;
import java.util.*;

public class Parser {
    private static final String DB_ROOT = "databases";
    static BST bst;
    //add protected keywords that relate to commands

    public static void parse(String command, DatabaseManager dbManager) {
        Scanner scanner = new Scanner(System.in);
        String[] parts = command.split("\\s+");


        if (parts.length < 2) {
            System.out.println("Invalid command.");
            return;
        }
        String temp = command;
        while (!command.contains(";")) {
            command = scanner.nextLine().trim();
            if(command.contains(";")){
                command = temp + command;
                continue;
            }
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
                try {
                    dbManager.useDatabase(parts[1].replace(";", ""));
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("Invalid input! Enter a valid Database name");
                }

                break;
            case "SELECT":
                String selectTable = "";
                int fromIndex;
                int whereIndex;
                int start;
                int end;
                int size;
                int index = 0;
                //find FROM index
                fromIndex = findFromIndex(parts);
                //find WHERE index

                whereIndex = findWhereIndex(parts);
                if (parts.length < 4 || (parts[1].equalsIgnoreCase("*") && parts[fromIndex].equalsIgnoreCase("FROM"))) {
                    //returns results for select * from tablename index;
                    selectTable = parts[3].replace(";", "");
                    TableManager.selectAll(selectTable, dbManager.getCurrentDatabase());
                } else if (parts.length >= 3) {
                    if (parts[fromIndex].equalsIgnoreCase("FROM") && !parts[whereIndex].equalsIgnoreCase("WHERE")) {
                        selectTable = parts[fromIndex + 1].replace(";", "");
                        start = 1;
                        end = fromIndex - 1;
                        size = 0;

                        for (int i = start; i <= end; i++) {
                            size++;
                        }
                        String[] attribute = new String[size];
                        for (int i = 1; i < parts[fromIndex - 1].length(); i++) {
                            if (i <= attribute.length) {
                                attribute[i - 1] = parts[i];
                                attribute[i - 1] = attribute[i - 1].replace(",", "").trim();
                            }
                        }
                        TableManager.selectAttribute(selectTable, dbManager, attribute);
                    } else if (parts[fromIndex].equalsIgnoreCase("FROM") && parts[whereIndex].equalsIgnoreCase("WHERE")) {
                        selectTable = parts[fromIndex + 1];

                        //find size of attribute array
                        start = 1;
                        end = fromIndex - 1;
                        size = 0;
                        for (int i = start; i <= end; i++) {
                            size++;
                        }
                        //add attributes from command to attribute array
                        String[] attribute = new String[size];
                        for (int i = 1; i < parts[fromIndex - 1].length(); i++) {
                            if (i <= attribute.length) {
                                attribute[i - 1] = parts[i];
                            }
                        }

                        //find size of conditional array
                        start = whereIndex + 1;
                        end = parts.length;
                        size = 0;
                        for (int i = start; i <= end; i++) {
                            size++;
                        }
                        String[] conditions = new String[size-1];
                        //add conditions to conditions array
                        for (int i = start; i < end; i++) {
                            if (index <= conditions.length) {
                                conditions[index] = parts[i].trim().replace(";", "");
                                conditions[index] = conditions[index].replace("\"", "");
                                conditions[index] = conditions[index].replace(";", "");
                            }
                            index++;
                        }

                        TableManager.conditionalSelection(selectTable, dbManager, attribute, conditions);

                    }
                } else {
                    System.out.println("Syntax Error: Use SELECT <AttrNameList> FROM <tableName> WHERE [Condition];");
                    return;
                }
                break;
            case "INSERT":
                String tableName = "";
                scanner = new Scanner(System.in);
                tableName = parts[1];
                if (!isValidStringLength(tableName)) {
                    System.out.println("Table name is too Long");
                    return;
                }
                if (dbManager.getCurrentDatabase() == null) {
                    System.out.println("No Database selected.");
                    System.out.print("MiniDB > ");
                    String databaseCommand = scanner.nextLine();
                    parse(databaseCommand, dbManager);
                    parseInsertValues(command.replace(";", ""), dbManager, tableName);
                    scanner.close();
                } else {
                    parseInsertValues(command.replace(";", ""), dbManager, tableName);
                }
                break;
            case "DELETE":
                if(dbManager.getCurrentDatabase() == null){
                    System.out.println("No Database was selected.");
                    return;
                }else{
                    TableManager.handleDelete(command, dbManager.getCurrentDatabase());
                }
                break;
            case "UPDATE":
                TableManager.handleUpdate(command, dbManager.getCurrentDatabase());
                break;
            default:
                System.out.println("Unknown command: " + parts[0]);
        }

    }

    public static void parseInsertValues(String command, DatabaseManager dbManager, String tableName){
        int start = command.indexOf("(");
        int end = command.lastIndexOf(")");
        String stringValues = command.substring(start + 1, end).trim();
        String[] values = stringValues.split(",");
        File dbFolder = new File(DB_ROOT + File.separator + dbManager.getCurrentDatabase());
        if (!dbFolder.exists()) {
            System.out.println("Database directory does not exist.");
        }

        File attributeFile = new File(dbFolder, tableName + "Attribute.txt");
        boolean exists = attributeFile.exists();

        if (start == -1 || end == -1 || end < start) {
            System.out.println("Syntax Error: Attributes must be enclosed in parentheses.");
            return;
        }

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
    public static int findFromIndex(String[] command){
        int index = 0;
        for(int i = 0; i < command.length;i++){
            if(command[i].equalsIgnoreCase("FROM")){
                index = i;
            }
        }
        return index;
    }
    public static int findWhereIndex(String[] command){
        int index = 0;
        for(int i = 0; i < command.length; i++){
            if(command[i].equalsIgnoreCase("WHERE")){
                index = i;
            }
        }
        return index;
    }
}
