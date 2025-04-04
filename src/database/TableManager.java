package database;

import java.io.*;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

public class TableManager {
    private static final String DB_ROOT = "databases";
    private String tableName = "";

    public static void createTable(DatabaseManager dbManager, String tableName, String[] attributes, String primaryKey) {

        if (dbManager.getCurrentDatabase() == null) {
            System.out.println("No database selected.");
            return;
        }

        // Sanitize table name
        tableName = sanitizeName(tableName);
        if (tableName.isEmpty()) {
            System.out.println("Invalid table name.");
            return;
        }

        File dbFolder = new File(DB_ROOT + File.separator + dbManager.getCurrentDatabase());
        if (!dbFolder.exists()) {
            System.out.println("Database directory does not exist.");
            return;
        }

        //Define file paths
        File attributeFile = new File(dbFolder, tableName + "Attribute.txt");
        File recordsFile = new File(dbFolder, tableName + "Records.txt");
        File indexFile = new File(dbFolder, tableName + "Index.txt");


        if(attributeFile.exists() || recordsFile.exists() || indexFile.exists()) {
            System.out.println("Table "+ tableName + " already exists.");
            return;
        }

        try {
            //Create attribute file(schema storage)
            if(attributeFile.createNewFile()) {
                try (FileWriter writer = new FileWriter(attributeFile)){
                    writer.write("Table: " + tableName + "\n");
                    writer.write("Primary Key: " + primaryKey + "\n");
                    //writer.write("Attributes:\n");

                    // Write each attribute on a new line
                    for (String attribute : attributes) {
                        writer.write("Attributes: " + attribute.trim() + "\n");
                    }
                }
            }

           //creates the file with header
            try(FileWriter writer = new FileWriter(recordsFile)){

                //write the header with attribute name , aligned in column
                int columnWidth = 20;


                // Write each attribute name on a new line with padding for alignment
                for (String attribute : attributes) {
                    String[] parts = attribute.trim().split(" ");
                    String attributeName = parts[0]; // Get the name of the attribute (e.g., id, name)
                    writer.write(String.format("%-" + columnWidth + "s", attributeName)); // Adjust alignment using columnWidth
                }
                writer.write("\n"); // Add a newline after the header

                System.out.println("Records file created with header: " + recordsFile.getName());
            }catch(IOException e) {
                System.out.println("Error writing records file: " + e.getMessage());
            }

            // Create empty index file
            if (indexFile.createNewFile()) {
                System.out.println("Index file created: " + indexFile.getName());
            }

            System.out.println("Table " + tableName + " created with primary key (" + primaryKey + ").");

        } catch (IOException e) {
            System.out.println("Error creating table files: " + e.getMessage());
        }
    }


    private static String sanitizeName(String name) {
        return name.replaceAll("[^a-zA-Z0-9_-]", ""); // Only allow letters, numbers, underscores, and dashes
    }
    //continue here
    public static boolean checkDataTypeInTable(String tableName, String[] requiredDatatype){
        boolean isNotValid = true;
        int length = requiredDatatype.length;
        int index = 0;
        try{
            //read from file
            BufferedReader reader = new BufferedReader(new FileReader(tableName + "Attribute.txt"));
            String line;
            String[] part;
            String[] dataType = new String[length];
            ArrayList<String> name = new ArrayList<>();
            String primaryKeyName = "";
            String primaryKeyDataType = "";
            while ((line = reader.readLine().toUpperCase()) != null) {
                part = line.split(" ");
                if(line.contains("PRIMARY KEY:")){
                    primaryKeyName = part[2];
                    primaryKeyDataType = part[3];
                    dataType[0] = part[3];
                }
                if (line.contains("ATTRIBUTES:")) {

                    if(!((part[1].equalsIgnoreCase(primaryKeyName)) && (part[3].equalsIgnoreCase(primaryKeyDataType)))){
                        name.add(part[1]);//might change this
                        dataType[index] = part[2];
                    }
                }
                index++;
            }
            reader.close();

            if (dataType.length != requiredDatatype.length) {
                return false;
            }

            for(int i = 0; i< dataType.length; i++){
                String datatype  = dataType[i];
                String value = requiredDatatype[i];

                switch (datatype){
                    case "integer":
                        if(!isInteger(value)){
                            return false;
                        }
                        break;

                    case "float":
                        if(!isFloat(value)){
                            return false;
                        }
                        break;
                    case "text":
                        break;
                    default:
                        return false;
                }
            }
            return true;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isInteger(String integer){
        try{
            int result = Integer.parseInt(integer);
            is16Bit(result);
            is32Bit(result);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    public static boolean is16Bit(int value) {
        return value >= Short.MIN_VALUE && value <= Short.MAX_VALUE;
    }

    public static boolean is32Bit(int value) {
        return value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE;
    }

    public static boolean isFloat(String value){
        try {
            Float.parseFloat(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
