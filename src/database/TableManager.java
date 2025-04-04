package database;

import java.io.*;
import java.util.Arrays;

import static database.Parser.bst;

public class TableManager {
    private static final String DB_ROOT = "C:\\Users\\sweet\\Downloads\\Mini Database\\Mini Database\\databases";
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
                    writer.write(tableName + "\n");
                    //writer.write(primaryKey + " primary key" + "\n");
                    //writer.write("Attributes:\n");

                    // Write each attribute on a new line
                    for (String attribute : attributes) {
                        writer.write(attribute.trim() + "\n");
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
    public static boolean checkDataTypeInTable(String tableName, String[] requiredValues, String dbManager){
        boolean isValid = true;
        int length = requiredValues.length;
        int index = 0;
        String line;
        String[] part;
        String[] dataType = new String[length];
        String[] name = new String[length];
        String primaryString = "";
        String primaryStringValue = "";
        String path = "C:\\Users\\sweet\\Downloads\\Mini Database\\Mini Database\\databases\\"+ dbManager.toLowerCase() + "\\" + tableName.toLowerCase() +"Attribute.txt";
        try{
            //read from file
            BufferedReader reader = new BufferedReader(new FileReader(path));
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                if(line.contains("key")){
                    String[] primaryPart = line.split(" ");
                    primaryString = primaryPart[0];
                }
                String upperLine = line.toUpperCase();
                part = upperLine.split(" ");

                if(index < length){
                    name[index] = part[0];
                    dataType[index] = part[1];
                }
                index++;
            }
            reader.close();

            if (dataType.length != requiredValues.length) {
                return false;
            }
            for(int i = 0; i < length; i++){
                if (name[i].equalsIgnoreCase(primaryString)) {
                    primaryStringValue = requiredValues[i];
                }
            }
            if(primaryIsValid(primaryString, primaryStringValue, tableName, dbManager)){
                for(int i = 0; i< dataType.length; i++){
                    String datatype  = dataType[i];
                    String value = requiredValues[i];

                    switch (datatype.toLowerCase()){
                        case "integer":
                            if(!isInteger(value.trim())){
                                return false;
                            }
                            break;
                        case "float":
                            if(!isFloat(value.trim())){
                                return false;
                            }
                            break;
                        case "text":
                            if(!charConstraintCheck(value.trim())){
                                return false;
                            }
                            break;
                        case"string":
                            if(!charConstraintCheck(value.trim())){
                                return false;
                            }
                            break;
                        default:
                            return false;
                    }
                }
                return true;
            }else{
                System.out.println("Duplicate primary keys found. Cannot insert into the table.");
                return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static boolean primaryIsValid(String primaryString, String primaryStringValue, String tableName, String dbManager) {
        //reads and finds primary key record from the file
        // finds the index that the primary key is located +
        //if line does not contain values return true;
        //reads through every line and adds the value at the record index in an array +
        //compare the saved value in array with the primary index in parameters
        //if the values are the same -> we can not add it to the file
        //else continue and add to file
        String line;
        String[] part;
        String[] values;
        String[] temp;
        int index = 0;
        String upperLine;
        int count = 0;
        String path = "C:\\Users\\sweet\\Downloads\\Mini Database\\Mini Database\\databases\\"+ dbManager.toLowerCase() + "\\" + tableName.toLowerCase() +"Records.txt";

        try {
            //find the index of primary key
            BufferedReader reader = new BufferedReader(new FileReader(path));
            int value = 0;
            while ((line = reader.readLine()) != null) {
                if (line.contains(primaryString)) {
                    upperLine = line.toUpperCase();
                    part = upperLine.split(" ");
                    index = findIndex(part, primaryString);
                }
                count++;
            }
            reader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        try{
            BufferedReader reader = new BufferedReader(new FileReader(path));
            values = new String[count-1];
            count =  0;
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                if(count < values.length){
                    upperLine = line.toUpperCase();
                    part = upperLine.split(" ");
                    values[count] = part[index-1];
                }
                count++;
            }
            reader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        for(int i = 0; i < values.length; i++){
            if(values[i].equalsIgnoreCase(primaryStringValue.trim())){
                return false;
            }
        }
        bst.insert(Integer.parseInt(primaryString), bst.getRecordPointer(primaryString));
        return true;
    }
    public static int findIndex(String[] arr, String target) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i].toLowerCase().contains(target)) {
                return i;
            }
        }
        return -1;
    }

    public static boolean isInteger(String integer){
        try{
            int result = Integer.parseInt(integer);
            if(integerSizeConstraintCheck(result)){
                return true;
            }
            else{
                System.out.println("Integer is too large.");
                return false;
            }
        } catch (NumberFormatException e) {
            return false;
        }
    }
    public static boolean charConstraintCheck(String string){
        if(string.length() > Math.pow(string.length(), 100)){
            return false;
        }
        return true;
    }
    public static boolean integerSizeConstraintCheck(int value) {
        return value >= Short.MIN_VALUE && value <= Integer.MAX_VALUE;
    }

    public static boolean isFloat(String value){
        try {
            Float.parseFloat(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static void writeValues(String tableName, String[] values, String dbManager) {
        int columnWidth = 20;
        String path = "C:\\Users\\sweet\\Downloads\\Mini Database\\Mini Database\\databases\\"+ dbManager.toLowerCase() + "\\" + tableName.toLowerCase() +"Records.txt";

        try{
            BufferedWriter writer = new BufferedWriter(new FileWriter(path));
            writer.write("\n");
            for(String value: values){
                String[] parts = value.trim().split(" ");
                String attributeName = parts[0];
                writer.write(String.format("%-" + columnWidth + "s", attributeName));
            }
            writer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
