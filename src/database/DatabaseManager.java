package database;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

	public void renameDatabase(File attFile, File recFile, List<String> attributes) {
		List<String> lines2Print = readFromAtt(attFile, attributes);
		if (lines2Print == null)
			return;
		outToFile(lines2Print, attFile);
		lines2Print = readFromRecords(recFile, attributes);
		outToFile(lines2Print, recFile);
		System.out.println("Successfully renamed attribute(s)!");
	}

	public List<String> readFromRecords(File fileName, List<String> attributes) {
		Scanner scan = null;
		String line;
		List<String> lines = new ArrayList<>();
		try {
			scan = new Scanner(new FileInputStream(fileName));
			line = scan.nextLine();
			line = "";
			for (int i = 0; i < attributes.size(); i++) {
				line += attributes.get(i) + "                ";
			}
			lines.add(line);
			while (scan.hasNext()) {
				line = scan.nextLine();
				lines.add(line);
			}
		} catch (Exception e) {
			System.out.println("Error " + e);
		} finally {
			scan.close();
		}
		return lines;
	}

	public void outToFile(List<String> lines, File fileName) {
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(fileName));
			for (int i = 0; i < lines.size(); i++) {
				writer.write(lines.get(i) + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public List<String> readFromAtt(File fileName, List<String> attributes) {

		Scanner scan = null;
		Scanner scan2 = null;
		String line;
		List<String> lines = new ArrayList<>();
		int numLinesFile = 0;
		int numLinesInput = 0;
		try {
			scan = new Scanner(new FileInputStream(fileName));
			scan2 = new Scanner(new FileInputStream(fileName));
			line = scan.nextLine();
			while (scan.hasNext()) {
				line = scan.nextLine();
				numLinesFile++;
			}
			for (String attribute : attributes) {
				numLinesInput++;
			}
			if (numLinesFile != numLinesInput) {
				System.out.println("Error: incorrect number of attributes given");
				return null;
			}
			line = scan2.nextLine();
			lines.add(line);
			String[] fileLine;
			for (int i = 0; i < numLinesFile; i++) {
				line = scan2.nextLine();
				fileLine = line.split(" ");
				fileLine[0] = attributes.get(i);
				String combineLine = "";
				for (int j = 0; j < fileLine.length; j++) {
					combineLine += fileLine[j] + " ";
				}
				lines.add(combineLine);
			}
		} catch (Exception e) {
			System.out.println("Error " + e);
		} finally {
			scan.close();
			scan2.close();
		}
		return lines;
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
		File dbAtt = new File(
				DB_ROOT + File.separator + getCurrentDatabase() + File.separator + tblName + "Attribute.txt");
		if (tblName.equalsIgnoreCase("ALL")) {
			// https://www.tutorialspoint.com/how-to-get-list-of-all-files-folders-from-a-folder-in-java
			File dirPath = new File(DB_ROOT + File.separator + getCurrentDatabase());
			File[] allFiles = dirPath.listFiles();
			for (int i = 0; i < allFiles.length; i++) {
				String fileName = allFiles[i].getName();
				if (fileName.contains("Attribute.txt"))
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
				line = scan.nextLine();
				System.out.println(line.toUpperCase());
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
