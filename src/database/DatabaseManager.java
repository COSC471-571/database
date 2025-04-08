package database;

import java.awt.*;
import java.io.File;
import java.io.FileInputStream;
import java.util.Scanner;

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
		if (!isValidStringLength(dbName)) {
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

	public static boolean isValidStringLength(String string) {
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

	public void describeDatabase(String tblName) {
		File dbAtt = new File(DB_ROOT + File.separator + getCurrentDatabase() + File.separator + tblName + "Attribute.txt");
		if (tblName.equalsIgnoreCase("ALL")) {
			//https://www.tutorialspoint.com/how-to-get-list-of-all-files-folders-from-a-folder-in-java
			File dirPath = new File(DB_ROOT + File.separator + getCurrentDatabase());
			File[] allFiles = dirPath.listFiles();
			for (int i=0; i<allFiles.length; i++) {
				String fileName = allFiles[i].getName();
				if(fileName.contains("Attribute.txt") )
					readFileForDescribe(allFiles[i]);					
			}
		} else
			readFileForDescribe(dbAtt);
	}
	public void readFileForDescribe(File dbAtt) {
		if (dbAtt.exists() && dbAtt.isFile()) { // https://stackoverflow.com/questions/1816673/how-do-i-check-if-a-file-exists-in-java
			String line;
			Scanner scan = null;
			try {
				scan = new Scanner(new FileInputStream(dbAtt));
				System.out.println();
				while (scan.hasNext()) {
					line = scan.nextLine();
					System.out.println(line);
				}
				System.out.println();
			} catch (Exception e) {
				System.out.println("Error " + e);
			} finally {
				scan.close();
			}
		} else {
			System.out.println("Table does not exist");
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
