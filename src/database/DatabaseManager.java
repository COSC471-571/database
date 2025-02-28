package database;



import java.io.*;
import java.util.*;


public class DatabaseManager {
    private BST databaseTree = new BST();
    private String currentDatabase = null;
    private static final String DB_ROOT = "databases"; // Root directory for databases
    
   

    public DatabaseManager() {
        File rootDir = new File(DB_ROOT);
        if (!rootDir.exists()) {
            rootDir.mkdir(); // Create the root directory if it does not exist
        }
    }

    public void createDatabase(String dbName) {
        if (dbName.endsWith(";")) {
            dbName = dbName.substring(0, dbName.length() - 1); // Remove semicolon
        }
        File dbFolder = new File(DB_ROOT + File.separator + dbName);
        
        if (dbFolder.exists()) {
            System.out.println("Database " + dbName + " already exists.");
        } else {
            if (dbFolder.mkdir()) {    
                System.out.println("Database " + dbName + " created.");
            } else {
                System.out.println("Error: Could not create database folder.");
            }
        }
    }
    
 

    public void useDatabase(String dbName) {
        if (dbName.endsWith(";")) {
            dbName = dbName.substring(0, dbName.length() - 1); // Remove semicolon
        }
        File dbFolder = new File(DB_ROOT + File.separator + dbName);
        
        if (dbFolder.exists() && dbFolder.isDirectory()) {
            currentDatabase = dbName;
            //System.out.print("Database changed\nMariaDB [" + dbName + "]> ");
        } else {
            System.out.println("Database " + dbName + " does not exist.");
        }
    }

    public String getCurrentDatabase() {
        return currentDatabase;
    }
    
    public void createTable(String tableName, String[] attributes, String primaryKey) {
        if (currentDatabase == null) {
            System.out.println("No database selected.");
            return;
        }

        // Ensure the table name is clean and does not have a semicolon
        if (tableName.endsWith(";")) {
            tableName = tableName.substring(0, tableName.length() - 1);
        }

        // Create a folder for the table inside the selected database
        File tableFolder = new File(DB_ROOT + File.separator + currentDatabase + File.separator + tableName);
        
        if (tableFolder.exists()) {
            System.out.println("Table " + tableName + " already exists.");
            return;
        } else {
            if (tableFolder.mkdir()) {
                System.out.println("Table " + tableName + " created.");
                // Now, create the BST for indexing based on the primary key
                BST tableTree = new BST(tableName, primaryKey);

                // Store attributes for future use (e.g., inserts)
                for (String attr : attributes) {
                    System.out.println("Attribute: " + attr);
                }

                System.out.println("Table " + tableName + " created with primary key (" + primaryKey + ").");
            } else {
                System.out.println("Error: Could not create table folder.");
            }
        }
    }

}

