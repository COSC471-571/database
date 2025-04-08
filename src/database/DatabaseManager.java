package database;



import java.awt.*;
import java.io.File;

public class DatabaseManager {
    private static final String DB_ROOT = "databases";
    private String currentDatabase = null;
    private boolean isCreated;

    public DatabaseManager() {
        File rootDir = new File(DB_ROOT);
        if (!rootDir.exists()) {
            rootDir.mkdir();
        }
    }

    public void createDatabase(String dbName) {
        dbName = dbName.replace(";", "");
        File dbFolder = new File(DB_ROOT + File.separator + dbName);
        if(!isValidStringLength(dbName)){
            System.out.println(dbName + "is too long. Insert a shorter name.");
        }
        if (dbFolder.exists()) {
            System.out.println("Database " + dbName + " already exists.");
        } else if (dbFolder.mkdir()) {
            System.out.println("Database " + dbName + " created.");
        } else {
            System.out.println("Error: Could not create database.");
        }
    }
    public static boolean isValidStringLength(String string){
        if (string.length() > Math.pow(string.length(), 19)) {
            return false;
        }
        return true;
    }

    public void useDatabase(String dbName) {

        dbName = dbName.replace(";", "");
        File dbFolder = new File(DB_ROOT + File.separator + dbName);

        if (dbFolder.exists() && dbFolder.isDirectory()) {
            currentDatabase = dbName;
            setCreated(true);

        } else {
            System.out.println("Database " + dbName + " does not exist.");
            setCreated(false);
        }
    }


    public String getCurrentDatabase() {
        return currentDatabase;
    }

    public void setCreated(boolean created) {
        isCreated = created;
    }

    public boolean isCreated() {
        return isCreated;
    }
}

