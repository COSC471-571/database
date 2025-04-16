package database;

import java.io.*;
import java.util.*;

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


        if (attributeFile.exists() || recordsFile.exists() || indexFile.exists()) {
            System.out.println("Table " + tableName + " already exists.");
            return;
        }

        try {
            //Create attribute file(schema storage)
            if (attributeFile.createNewFile()) {
                try (FileWriter writer = new FileWriter(attributeFile)) {
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
            try (FileWriter writer = new FileWriter(recordsFile)) {

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
            } catch (IOException e) {
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

    public static boolean checkDataTypeInTable(String tableName, String[] requiredValues, DatabaseManager dbManager) {
        int length = requiredValues.length;
        int index = 0;
        String line;
        String[] part;
        String[] dataType = new String[length];
        String[] name = new String[length];
        String primaryString = "";
        String primaryStringValue = "";
        File dbFolder = new File(DB_ROOT + File.separator + dbManager.getCurrentDatabase());
        if (!dbFolder.exists()) {
            System.out.println("Database directory does not exist.");
            return false;
        }
        File attributeFile = new File(dbFolder, tableName + "Attribute.txt");

        try {
            //read from file
            BufferedReader reader = new BufferedReader(new FileReader(attributeFile));
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                if (line.contains("key")) {
                    String[] primaryPart = line.split(" ");
                    primaryString = primaryPart[0];
                }
                String upperLine = line.toUpperCase();
                part = upperLine.split(" ");

                if (index < length) {
                    name[index] = part[0];
                    dataType[index] = part[1];
                }
                index++;
            }
            reader.close();

            if (dataType.length != requiredValues.length) {
                return false;
            }
            for (int i = 0; i < length; i++) {
                if (name[i].equalsIgnoreCase(primaryString)) {
                    primaryStringValue = requiredValues[i];
                }
            }
            if (primaryIsValid(primaryString, primaryStringValue, tableName, dbManager)) {
                for (int i = 0; i < dataType.length; i++) {
                    String datatype = dataType[i];
                    String value = requiredValues[i];

                    switch (datatype.toLowerCase()) {
                        case "integer":
                            if (!isInteger(value.trim())) {
                                return false;
                            }
                            break;
                        case "float":
                            if (!isFloat(value.trim())) {
                                return false;
                            }
                            break;
                        case "text", "string":
                            if (!charConstraintCheck(value.trim())) {
                                return false;
                            }
                            break;
                        default:
                            return false;
                    }
                }
                return true;
            } else {
                System.out.println("Duplicate primary keys found. Cannot insert into the table.");
                return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static boolean primaryIsValid(String primaryString, String primaryStringValue, String tableName, DatabaseManager dbManager) {
        String line;
        String[] part;
        String[] values;
        String[] temp;
        int index = 0;
        String upperLine;
        int count = 0;
        File dbFolder = new File(DB_ROOT + File.separator + dbManager.getCurrentDatabase());
        if (!dbFolder.exists()) {
            System.out.println("Database directory does not exist.");
            return false;
        }
        File recordsFile = new File(dbFolder, tableName + "Records.txt");

        try {
            //find the index of primary key
            BufferedReader reader = new BufferedReader(new FileReader(recordsFile));
            int value = 0;
            while ((line = reader.readLine()) != null) {
                if (line.contains(primaryString)) {
                    upperLine = line.toUpperCase().trim();
                    part = upperLine.split(" ");
                    index = findIndex(part, primaryString);
                }
                count++;
            }
            reader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        try {
            BufferedReader reader = new BufferedReader(new FileReader(recordsFile));
            values = new String[count - 1];
            count = 0;
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                if (count < values.length) {
                    upperLine = line.toUpperCase();
                    part = upperLine.split(" ");
                    values[count] = part[index];
                }
                count++;
            }
            reader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < values.length; i++) {
            if (values[i].equalsIgnoreCase(primaryStringValue.trim())) {
                return false;
            }
        }
        //bst.insert(Integer.parseInt(primaryString), bst.getRecordPointer(primaryString));
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

    public static boolean isInteger(String integer) {
        try {
            int result = Integer.parseInt(integer);
            if (integerSizeConstraintCheck(result)) {
                return true;
            } else {
                System.out.println("Integer is too large.");
                return false;
            }
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static boolean charConstraintCheck(String string) {
        if (string.length() > Math.pow(string.length(), 100)) {
            return false;
        }
        return true;
    }

    public static boolean integerSizeConstraintCheck(int value) {
        return value >= Short.MIN_VALUE && value <= Integer.MAX_VALUE;
    }

    public static boolean isFloat(String value) {
        try {
            Float.parseFloat(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static void writeValues(String tableName, String[] values, DatabaseManager dbManager) {
        int columnWidth = 20;
        File dbFolder = new File(DB_ROOT + File.separator + dbManager.getCurrentDatabase());
        if (!dbFolder.exists()) {
            System.out.println("Database directory does not exist.");
        }
        File recordsFile = new File(dbFolder, tableName + "Records.txt");
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(recordsFile, true));
            for (String value : values) {
                String[] parts = value.trim().split(" ");
                String attributeName = parts[0];
                writer.write(String.format("%-" + columnWidth + "s", attributeName));
            }
            writer.write("\n");
            writer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void selectAll(String tableName, String dbManager) {
        String path = "databases" + File.separator + dbManager.toLowerCase() + File.separator + tableName.toLowerCase() + "Records.txt";

        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            System.out.println("Error reading records file: " + e.getMessage());
        }
    }

    public static void selectAttribute(String tableName, DatabaseManager databaseManager, String[] attributes) {
        String path = "databases" + File.separator + databaseManager.getCurrentDatabase() + File.separator + tableName.toLowerCase() + "Records.txt";
        String line;
        StringBuilder attributeList = new StringBuilder();
        int columnWidth = 20;
        String[] split;
        int[] indices;
        StringBuilder lineData;
        String temp = "";
        for (int i = 0; i < attributes.length; i++) {
            if (attributes[i] == temp) {
                System.out.println("Invalid selection: duplicate attributes found.");
                return;
            }
            temp = attributes[i];
        }
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));

            //find the index of the listed attributes
            line = reader.readLine().trim();
            split = line.split("\\s+");
            int index = 0;
            indices = new int[attributes.length];
            for (int i = 0; i < attributes.length; i++) {
                for (int j = 0; j < split.length; j++) {
                    if (attributes[i].equalsIgnoreCase(split[j])) {
                        indices[index] = j;
                        index++;
                        attributeList.append(String.format("%-" + columnWidth + "s", attributes[i]));
                    }
                }
            }
            System.out.println(attributeList.toString());

            while ((line = reader.readLine()) != null) {
                lineData = new StringBuilder();
                temp = line.replace("\"", "").trim();
                split = temp.split("\\s+");
                for (int i = 0; i < indices.length; i++) {
                    if (indices[i] >= 0) {
                        lineData.append(String.format("%-" + columnWidth + "s", split[indices[i]]));
                    }
                }
                System.out.println(lineData.toString());
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static boolean isFull(String[] strings){
        boolean isFull = true;
        for (String string : strings) {
            if (string == null) {
                isFull = false;
                break;
            }
        }
        return isFull;
    }
    public static boolean hasNull(String[][] strings){
        boolean hasNull = false;
        for (int i = 0; i < strings.length; i++) {
            if (strings[i] == null) {
                hasNull = true;
                break;
            }
            for (int j = 0; j < strings[i].length; j++) {
                if (strings[i][j] == null) {
                    hasNull = true;
                    break;
                }
            }
        }
        return hasNull;
    }

    private static void compareRows(String tableName, DatabaseManager databaseManager, String[] attributesSelected, String[] relationalOp, String[] attributesFromWhereClause, String[] recordToLocate, String[] andOrArray) {
        String path = "databases" + File.separator + databaseManager.getCurrentDatabase() + File.separator + tableName.toLowerCase() + "Records.txt";
        String line;
        StringBuilder attributeList = new StringBuilder();
        String[] dbRow;
        int[] indices;
        int[] headerIndex;
        String removedQuotes = "";
        String[] booleanArray = new String[attributesFromWhereClause.length];
        String[] storedRow = new String[attributesFromWhereClause.length];
        boolean isValidRow = true;
        String temp = "";
        int columnWidth = 20;
        int numberOfSeletedRows = 0;

        for (int i = 0; i < attributesSelected.length; i++) {
            if (attributesSelected[i] == temp) {
                System.out.println("Invalid selection: duplicate attributes found.");
                return;
            }
            temp = attributesSelected[i];
        }
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));

            //find the index of the listed attributes with the attributes array
            line = reader.readLine().trim();
            line = line.replace("\"", "").trim();
            //System.out.println(String.format("%-" + columnWidth + "s", line));
            dbRow = line.split("\\s+");
            int index = 0;
            StringBuilder lineData;
            indices = new int[attributesFromWhereClause.length];
            for (int j = 0; j < attributesFromWhereClause.length; j++) {
                for (int i = 0; i < dbRow.length; i++) {
                    if (attributesFromWhereClause[j].equalsIgnoreCase(dbRow[i])) {
                        indices[index] = i;
                        index++;
                    }
                }
            }
            headerIndex = new int[attributesSelected.length];
            index = 0;
            //get the index of the selected attributes in the header
            for (int j = 0; j < attributesSelected.length; j++) {
                attributesSelected[j] = attributesSelected[j].replace(",", "").trim();
                for (int i = 0; i < dbRow.length; i++) {
                    if (attributesSelected[j].equalsIgnoreCase(dbRow[i])) {
                        headerIndex[index] = i;
                        attributeList.append(String.format("%-" + columnWidth + "s", dbRow[headerIndex[index]]));
                        index++;
                    }
                }
            }
            System.out.println(attributeList.toString());
            int rowCount = 0;
            while ((line = reader.readLine()) != null) {
                Arrays.fill(booleanArray, null);
                lineData = new StringBuilder();
                removedQuotes = line.replace("\"", "").trim();
                dbRow = removedQuotes.split("\\s+");
                for (int j = 0; j < indices.length; j++) {
                    if (relationalOp[j].equalsIgnoreCase("=")) {
                        if (dbRow[indices[j]].equalsIgnoreCase(recordToLocate[j])) {
                            booleanArray[j] = "TRUE";
                        } else {
                            if(!isFull(booleanArray)){
                                booleanArray[j] = "FALSE";

                            }
                        }
                    }
                    if(relationalOp[j].equalsIgnoreCase("!=")){
                        if (!dbRow[indices[j]].equalsIgnoreCase(recordToLocate[j])) {
                            booleanArray[j] = "TRUE";
                        } else {
                            if(!isFull(booleanArray)){
                                booleanArray[j] = "FALSE";
                            }
                        }
                    }
                    if(relationalOp[j].equalsIgnoreCase("<")){
                        if(isInteger(recordToLocate[j]) && isInteger(dbRow[indices[j]])){
                            if(Integer.parseInt(dbRow[indices[j]]) < Integer.parseInt(recordToLocate[j])){
                                booleanArray[j] = "TRUE";
                            }else {
                                if(!isFull(booleanArray)){
                                    booleanArray[j] = "FALSE";

                                }
                            }
                        }
                        if(isFloat(recordToLocate[j]) && isFloat(dbRow[indices[j]])){
                            if(Float.parseFloat(dbRow[indices[j]]) < Float.parseFloat(recordToLocate[j])){
                                booleanArray[j] = "TRUE";
                            }else {
                                if(!isFull(booleanArray)){
                                    booleanArray[j] = "FALSE";

                                }
                            }
                        }
                        else{
                            System.out.println("Can not use '<' on strings");
                            return;
                        }

                    }
                    if(relationalOp[j].equalsIgnoreCase(">")){
                        if(isInteger(recordToLocate[j]) && isInteger(dbRow[indices[j]])){
                            if(Integer.parseInt(dbRow[indices[j]]) > Integer.parseInt(recordToLocate[j])){
                                booleanArray[j] = "TRUE";
                            }else {
                                if(!isFull(booleanArray)){
                                    booleanArray[j] = "FALSE";

                                }
                            }
                        }
                        if(isFloat(recordToLocate[j]) && isFloat(dbRow[indices[j]])){
                            if(Float.parseFloat(dbRow[indices[j]]) > Float.parseFloat(recordToLocate[j])){
                                booleanArray[j] = "TRUE";
                            }else {
                                if(!isFull(booleanArray)){
                                    booleanArray[j] = "FALSE";

                                }
                            }
                        }
                        else{
                            System.out.println("Can not use '>' on strings");
                            return;
                        }

                    }
                    if(relationalOp[j].equalsIgnoreCase("<=")){
                        if(isInteger(recordToLocate[j]) && isInteger(dbRow[indices[j]])){
                            if(Integer.parseInt(dbRow[indices[j]]) <= Integer.parseInt(recordToLocate[j])){
                                booleanArray[j] = "TRUE";
                            }else {
                                if(!isFull(booleanArray)){
                                    booleanArray[j] = "FALSE";

                                }
                            }
                        }
                        if(isFloat(recordToLocate[j]) && isFloat(dbRow[indices[j]])){
                            if(Float.parseFloat(dbRow[indices[j]]) <= Float.parseFloat(recordToLocate[j])){
                                booleanArray[j] = "TRUE";
                            }else {
                                if(!isFull(booleanArray)){
                                    booleanArray[j] = "FALSE";

                                }
                            }
                        }
                        else{
                            System.out.println("Can not use '<=' on strings");
                            return;
                        }

                    }
                    if(relationalOp[j].equalsIgnoreCase(">=")){
                        if(isInteger(recordToLocate[j]) && isInteger(dbRow[indices[j]])){
                            if(Integer.parseInt(dbRow[indices[j]]) >= Integer.parseInt(recordToLocate[j])){
                                booleanArray[j] = "TRUE";
                            }else {
                                if(!isFull(booleanArray)){
                                    booleanArray[j] = "FALSE";

                                }
                            }
                        }
                        if(isFloat(recordToLocate[j]) && isFloat(dbRow[indices[j]])){
                            if(Float.parseFloat(dbRow[indices[j]]) >= Float.parseFloat(recordToLocate[j])){
                                booleanArray[j] = "TRUE";
                            }else {
                                if(!isFull(booleanArray)){
                                    booleanArray[j] = "FALSE";

                                }
                            }
                        }
                        else{
                            System.out.println("Can not use '>=' on strings");
                            return;
                        }

                    }
                }
                //prints valid row
                if (isValidRow(andOrArray, booleanArray)) {
                    index = rowCount;
                    System.out.print(index + "." );
                    for(int i = 0; i < headerIndex.length; i++){
                        lineData.append(String.format("%-" + columnWidth + "s", dbRow[headerIndex[i]]));

                    }
                    System.out.println(lineData.toString());
                    numberOfSeletedRows++;
                }
                rowCount++;
            }
            if(numberOfSeletedRows == 0){
                System.out.println("Nothing found");
            }
            System.out.println();
            reader.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static boolean isNull(String[]  strings){
        if(strings == null || strings.length < 1){
            return true;
        }
        return false;
    }

    public static void multipleTableSelection(String[] tableNames, DatabaseManager dbManager, String[] attributesFromSelectCommand, String[] conditions) {
        int size;
        String[] andORArray = new String[findSizeOfAndOrArray(conditions)];
        if(checkForDuplicateSelectedAttributes(attributesFromSelectCommand)){
            System.out.println("Invalid selection: duplicate attributes found.");
            return;
        }
        //loop through each table name and perform each step
        for(int i = 0; i < tableNames.length; i++) {
            //find header size
            String tablename = tableNames[i];
            size = TableManager.findHeaderSize(dbManager, tablename, conditions);
            //set size to arrays
            String[] attributesToCheck = new String[size];
            String[] valuesFromWhereClause = new String[size];
            String[] relationalOps = new String[size];
            //find all header attributes
            String[] headerAttributes = getHeaderAttributes(dbManager, tablename);
            //find all conditional attributes

            //locate operators
            relationalOps = getRelationalOp(conditions, headerAttributes, size);
            //locate attributes
            attributesToCheck = getAttributesFromWhereClause(conditions, headerAttributes, size);
            //locate record/value to test
            valuesFromWhereClause = getValueFromWhereClause(conditions, headerAttributes, size);
            //check booleans in the where clause and store results
            andORArray = andOrBooleanChecker(conditions);
            //pass arrays and information into new row selector method that checks multiple conditions are true or false
            //from different tables
            compareMultipleTableRows(tablename, dbManager, attributesFromSelectCommand, relationalOps, attributesToCheck, valuesFromWhereClause, andORArray);
        }
    }
    public static String[] compareMultipleTableRows(String tableName, DatabaseManager databaseManager, String[] attributesFromSelectCommand, String[] relationalOp, String[] attributesFromWhereClause, String[] valueFromWhereClause, String[] andOrArray){
        String[] booleanAndRowNumberResult = new String[attributesFromWhereClause.length];
        //find indexes of attributes from where clause
        int[]  whereClauseAttributesIndexes = getIndexesOfWhereClauseAttribute(databaseManager, tableName, attributesFromWhereClause);
        //find select clause attributes indexes
        int[] selectedAttributesfromSelectClauseIndexes = getSelectedAttributesIndex(attributesFromSelectCommand, databaseManager,tableName);
        //get valid rows and index
        String[] rows = getRowsFromTable(whereClauseAttributesIndexes, attributesFromWhereClause, valueFromWhereClause, relationalOp, tableName, databaseManager);



        return booleanAndRowNumberResult;
    }
    public static String[] getRowsFromTable(int[] whereClauseAttributesIndexes, String[] attributesFromWhereClause, String[] valueFromWhereClause, String[] relationalOp, String tableName, DatabaseManager databaseManager){
        String[] validRows = new String[attributesFromWhereClause.length];
        String path = "databases" + File.separator + databaseManager.getCurrentDatabase() + File.separator + tableName.toLowerCase() + "Records.txt";
        String line;
        String[] dbRow;
        String removedQuotes = "";
        String[] booleanArray = new String[attributesFromWhereClause.length];
        int rowCount = 0;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            while ((line = reader.readLine()) != null) {
                Arrays.fill(booleanArray, null);
                removedQuotes = line.replace("\"", "").trim();
                dbRow = removedQuotes.split("\\s+");
                for (int j = 0; j < whereClauseAttributesIndexes.length; j++) {
                    if (relationalOp[j].equalsIgnoreCase("=")) {
                        if (dbRow[whereClauseAttributesIndexes[j]].equalsIgnoreCase(valueFromWhereClause[j])) {
                            validRows[j] = "TRUE" + " " + rowCount;
                        }
                    }
                    if (relationalOp[j].equalsIgnoreCase("!=")) {
                        if (!dbRow[whereClauseAttributesIndexes[j]].equalsIgnoreCase(valueFromWhereClause[j])) {
                            validRows[j] = "TRUE" + " " + rowCount;
                        }
                    }
                    if (relationalOp[j].equalsIgnoreCase("<")) {
                        if (isInteger(valueFromWhereClause[j]) && isInteger(dbRow[whereClauseAttributesIndexes[j]])) {
                            if (Integer.parseInt(dbRow[whereClauseAttributesIndexes[j]]) < Integer.parseInt(valueFromWhereClause[j])) {
                                validRows[j] = "TRUE" + " " + rowCount;
                            }
                        }
                        if (isFloat(valueFromWhereClause[j]) && isFloat(dbRow[whereClauseAttributesIndexes[j]])) {
                            if (Float.parseFloat(dbRow[whereClauseAttributesIndexes[j]]) < Float.parseFloat(valueFromWhereClause[j])) {
                                validRows[j] = "TRUE" + " " + rowCount;
                            }
                        } else {
                            System.out.println("Can not use '<' on strings");
                        }

                    }
                    if (relationalOp[j].equalsIgnoreCase(">")) {
                        if (isInteger(valueFromWhereClause[j]) && isInteger(dbRow[whereClauseAttributesIndexes[j]])) {
                            if (Integer.parseInt(dbRow[whereClauseAttributesIndexes[j]]) > Integer.parseInt(valueFromWhereClause[j])) {
                                validRows[j] = "TRUE" + " " + rowCount;
                            }
                        }
                        if (isFloat(valueFromWhereClause[j]) && isFloat(dbRow[whereClauseAttributesIndexes[j]])) {
                            if (Float.parseFloat(dbRow[whereClauseAttributesIndexes[j]]) > Float.parseFloat(valueFromWhereClause[j])) {
                                validRows[j] = "TRUE" + " " + rowCount;
                            }
                        } else {
                            System.out.println("Can not use '>' on strings");
                        }

                    }
                    if (relationalOp[j].equalsIgnoreCase("<=")) {
                        if (isInteger(valueFromWhereClause[j]) && isInteger(dbRow[whereClauseAttributesIndexes[j]])) {
                            if (Integer.parseInt(dbRow[whereClauseAttributesIndexes[j]]) <= Integer.parseInt(valueFromWhereClause[j])) {
                                validRows[j] = "TRUE" + " " + rowCount;
                            }
                        }
                        if (isFloat(valueFromWhereClause[j]) && isFloat(dbRow[whereClauseAttributesIndexes[j]])) {
                            if (Float.parseFloat(dbRow[whereClauseAttributesIndexes[j]]) <= Float.parseFloat(valueFromWhereClause[j])) {
                                validRows[j] = "TRUE" + " " + rowCount;
                            }
                        } else {
                            System.out.println("Can not use '<=' on strings");
                        }

                    }
                    if (relationalOp[j].equalsIgnoreCase(">=")) {
                        if (isInteger(valueFromWhereClause[j]) && isInteger(dbRow[whereClauseAttributesIndexes[j]])) {
                            if (Integer.parseInt(dbRow[whereClauseAttributesIndexes[j]]) >= Integer.parseInt(valueFromWhereClause[j])) {
                                validRows[j] = "TRUE" + " " + rowCount;
                            }
                        }
                        if (isFloat(valueFromWhereClause[j]) && isFloat(dbRow[whereClauseAttributesIndexes[j]])) {
                            if (Float.parseFloat(dbRow[whereClauseAttributesIndexes[j]]) >= Float.parseFloat(valueFromWhereClause[j])) {
                                validRows[j] = "TRUE" + " " + rowCount;
                            }
                        } else {
                            System.out.println("Can not use '>=' on strings");
                        }

                    }
                }
            }
            for(int i = 0; i < validRows.length; i++){
                if (!isFull(validRows) || validRows[i] == null) {
                    validRows[i] = "FALSE";
                }
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return validRows;
    }

    public static int[] getIndexesOfWhereClauseAttribute(DatabaseManager databaseManager, String tableName, String[] attributesFromWhereClause){
        String path = "databases" + File.separator + databaseManager.getCurrentDatabase() + File.separator + tableName.toLowerCase() + "Records.txt";
        String line;
        String[] dbRow;
        int[] indices;

        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));

            //find the index of the listed attributes with the attributes array
            line = reader.readLine().trim();
            line = line.replace("\"", "").trim();
            dbRow = line.split("\\s+");
            int index = 0;
            indices = new int[attributesFromWhereClause.length];
            for (int j = 0; j < attributesFromWhereClause.length; j++) {
                for (int i = 0; i < dbRow.length; i++) {
                    if (attributesFromWhereClause[j].equalsIgnoreCase(dbRow[i])) {
                        indices[index] = i;
                        index++;
                    }
                }
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return indices;
    }
    public static boolean checkForDuplicateSelectedAttributes(String[] attributesSelected){
        String temp = "";
        for (int i = 0; i < attributesSelected.length; i++) {
            if (attributesSelected[i] != temp) {
                return false;
            }
            temp = attributesSelected[i];
        }
        return true;
    }
    public static String[] andOrBooleanChecker(String[] conditions){
        String conditionsString = String.join(" ", conditions);
        boolean containsAnd = false;
        boolean containsOr = false;
        boolean containsOrAnd = false;
        int index;

        String[] andORArray = new String[findSizeOfAndOrArray(conditions)];
        //boolean checker to see if the condition where statement contains and or or
        if(conditionsString.toUpperCase().contains("AND")){
            containsAnd = true;
        }
        else if(conditionsString.toUpperCase().contains("OR")){
            containsOr = true;
        }
        else if(conditionsString.toUpperCase().contains("AND") && conditionsString.toUpperCase().contains("OR")){
            containsOrAnd = true;
        }
        //if it does contain it we loop through the condition statement and store the and or or
        if(containsAnd || containsOr || containsOrAnd){
            index = 0;
            for(int i = 0; i < conditions.length; i++){
                if(conditions[i].equalsIgnoreCase("AND") || conditions[i].equalsIgnoreCase("OR")){
                    andORArray[index] = conditions[i];
                    index++;
                }
            }
        }
        return andORArray;
    }
    public static String[] getValueFromWhereClause(String[] conditions, String[] headerAttributes, int size){
        String[] recordToLocate = new String[size];
        int index = 0;
        String foundAttribute = "";
        String[] temp = new String[conditions.length];
        for(int i = 0; i < conditions.length; i++) {
            for (int j = 0; j < headerAttributes.length; j++) {
                if (conditions[i].equalsIgnoreCase(headerAttributes[j])) {
                    if (conditions[i + 1].equalsIgnoreCase("=")) {
                        recordToLocate[index] = conditions[i + 2]; //record to locate
                        index++;
                    }
                    if (conditions[i + 1].equalsIgnoreCase("!=")) {
                        recordToLocate[index] = conditions[i + 2]; //record to locate
                        index++;
                    }
                    if (conditions[i + 1].equalsIgnoreCase("<")) {
                        recordToLocate[index] = conditions[i + 2]; //record to locate
                        index++;
                    }
                    if (conditions[i + 1].equalsIgnoreCase(">")) {
                        recordToLocate[index] = conditions[i + 2]; //record to locate
                        index++;
                    }
                    if (conditions[i + 1].equalsIgnoreCase("<=")) {
                        recordToLocate[index] = conditions[i + 2]; //record to locate
                        index++;
                    }
                    if (conditions[i + 1].equalsIgnoreCase(">=")) {
                        recordToLocate[index] = conditions[i + 2]; //record to locate
                        index++;
                    }
                }
                if(conditions[i].contains("=") || conditions[i].contains("!=") || conditions[i].contains(">")
                        || conditions[i].contains(">=")|| conditions[i].contains("<")|| conditions[i].contains("<=")){
                    int number = 0;
                    if(!isFull(temp)){
                        temp = cleanedArray(temp[j], conditions[i]);
                    }
                    int k;
                    for(k = 0; k < temp.length; k++){
                        if(temp[k].toLowerCase().equalsIgnoreCase(headerAttributes[j])){
                            if(foundAttribute == ""){
                                foundAttribute = temp[k];
                                if(k == 0){
                                    number = 1;
                                }else if(k == 1){
                                    number = 2;
                                }
                            }

                        }
                    }

                    if(number == 1){
                        recordToLocate[index] = temp[1];
                        index++;
                    }
                    if(number == 2){
                        recordToLocate[index] = temp[0];
                        index++;
                    }


                }
            }
        }
        return recordToLocate;
    }
    public static String[] getAttributesFromWhereClause(String[] conditions, String[] headerAttributes, int size){
        String[] attributeToCheck = new String[size];
        int index = 0;
        String[] temp = new String[conditions.length];
        for(int i = 0; i < conditions.length; i++) {
            for (int j = 0; j < headerAttributes.length; j++) {
                if (conditions[i].equalsIgnoreCase(headerAttributes[j])) {
                    if (conditions[i + 1].equalsIgnoreCase("=")) {
                        attributeToCheck[index] = conditions[i]; //store the attributes to compare
                        index++;
                    }
                    if (conditions[i + 1].equalsIgnoreCase("!=")) {
                        attributeToCheck[index] = conditions[i]; //store the attributes to compare
                        index++;
                    }
                    if (conditions[i + 1].equalsIgnoreCase("<")) {
                        attributeToCheck[index] = conditions[i]; //store the attributes to compare
                        index++;
                    }
                    if (conditions[i + 1].equalsIgnoreCase(">")) {
                        attributeToCheck[index] = conditions[i]; //store the attributes to compare
                        index++;
                    }
                    if (conditions[i + 1].equalsIgnoreCase("<=")) {
                        attributeToCheck[index] = conditions[i]; //store the attributes to compare
                        index++;
                    }
                    if (conditions[i + 1].equalsIgnoreCase(">=")) {
                        attributeToCheck[index] = conditions[i]; //store the attributes to compare
                        index++;
                    }
                }
                if(conditions[i].contains("=") || conditions[i].contains("!=") || conditions[i].contains(">")
                        || conditions[i].contains(">=")|| conditions[i].contains("<")|| conditions[i].contains("<=")){
                    if(!isFull(temp)){
                        temp = cleanedArray(temp[j], conditions[i]);
                    }

                    for(int k = 0; k < temp.length; k++){
                        if(temp[k].toLowerCase().equalsIgnoreCase(headerAttributes[j])){
                            attributeToCheck[index] = temp[k];
                            index++;
                        }
                    }
                }
            }
        }
        return attributeToCheck;
    }
    public static String[] getRelationalOp(String[] conditions, String[] headerAttributes, int size){
        String relationalOp[] = new String[size];
        int index = 0;
        String[] temp = new String[conditions.length];
        for(int i = 0; i < headerAttributes.length; i++) {
            for (int j = 0; j < conditions.length; j++) {
                if (conditions[j].equalsIgnoreCase(headerAttributes[i])) {
                    if (conditions[j + 1].equalsIgnoreCase("=")) {
                        relationalOp[index] = conditions[j + 1]; //store relational operator
                        index++;
                    }
                    if (conditions[i + 1].equalsIgnoreCase("!=")) {
                        relationalOp[index] = conditions[i + 1]; //store relational operator
                        index++;
                    }
                    if (conditions[i + 1].equalsIgnoreCase("<")) {
                        relationalOp[index] = conditions[i + 1]; //store relational operator
                        index++;
                    }
                    if (conditions[i + 1].equalsIgnoreCase(">")) {
                        relationalOp[index] = conditions[i + 1]; //store relational operator
                        index++;
                    }
                    if (conditions[i + 1].equalsIgnoreCase("<=")) {
                        relationalOp[index] = conditions[i + 1]; //store relational operator
                        index++;
                    }
                    if (conditions[i + 1].equalsIgnoreCase(">=")) {
                        relationalOp[index] = conditions[i + 1]; //store relational operator
                        index++;
                    }
                }
                if(conditions[j].contains("=") || conditions[j].contains("!=") || conditions[j].contains(">")
                        || conditions[j].contains(">=")|| conditions[j].contains("<")|| conditions[j].contains("<=")){
                    String[] possibleOperators = {">=", "<=", "!=", ">", "<", "="};
                    String condition = conditions[j];
                    String foundOperator = null;

                    for (String op : possibleOperators) {
                        if (condition.contains(op)) {
                            foundOperator = op;
                            break;
                        }
                    }

                    if (foundOperator != null) {
                        for(int k = 0;  k < relationalOp.length; k++){
                            if(relationalOp[k] == null){
                                relationalOp[index] = foundOperator;
                                index++;
                            }
                        }

                    }

                }
            }
        }
        return relationalOp;
    }
    public static String[] getHeaderAttributes(DatabaseManager databaseManager, String tableName){
        String[] headerAttributes;
        String path = "databases" + File.separator + databaseManager.getCurrentDatabase() + File.separator + tableName.toLowerCase() + "Records.txt";
        String line;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            line = reader.readLine().trim();
            headerAttributes = line.split("\\s+");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return headerAttributes;
    }
    public static int findHeaderSize(DatabaseManager databaseManager, String tableName, String[] conditions){
        String path = "databases" + File.separator + databaseManager.getCurrentDatabase() + File.separator + tableName.toLowerCase() + "Records.txt";
        String line;
        String[] headerAttributes;
        int size = 0;
        DatabaseManager dbManager = databaseManager;
        String[] temp = new String[conditions.length];
        String cleaned = "";

        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            line = reader.readLine().trim();
            headerAttributes = line.split("\\s+");
            for (int i = 0; i < headerAttributes.length; i++) {
                for (int j = 0; j < conditions.length; j++) {
                    if(conditions[j].contains("=") || conditions[j].contains("!=") || conditions[j].contains(">")
                            || conditions[j].contains(">=")|| conditions[j].contains("<")|| conditions[j].contains("<=")){
                        if(!isFull(temp)){
                            temp = cleanedArray(temp[i], conditions[i]);
                        }
                        for(int k = 0; k < temp.length; k++){
                            if(temp[k].toLowerCase().equalsIgnoreCase(headerAttributes[i])){
                                size++;
                            }
                        }
                    }
                    if(conditions[j].toLowerCase().equalsIgnoreCase(headerAttributes[i])){
                        size++;
                    }
                }
            }
            return size;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static String[] cleanedArray(String string, String condition){
        string = Arrays.toString(condition.split("=" ));
        String cleaned = string.replaceAll("[\\[\\]]", "").trim();
        String[] cleanedArray = cleaned.split("\\s*,\\s*");
        return cleanedArray;

    }

    public static boolean isValidRow(String[] andOrArray, String[] booleanArray) {
        boolean isValidRow = false;
        String temp = "";
        if (!isNull(andOrArray)) {
            for (int i = 0; i < andOrArray.length; i++) {
                for (int j = 0; j < booleanArray.length; j++) {
                    if (andOrArray[i].equalsIgnoreCase("AND")) {
                        if (booleanArray[j].equalsIgnoreCase("TRUE") && temp.equalsIgnoreCase("TRUE")) {
                            isValidRow = true;
                        } else {
                            isValidRow = false;
                        }
                        temp = booleanArray[j];
                    }
                    if (andOrArray[i].equalsIgnoreCase("OR")) {
                        if (andOrArray.length == 1) {
                            if (booleanArray[j].toUpperCase().equalsIgnoreCase("TRUE") || temp.equalsIgnoreCase("TRUE")) {
                                isValidRow = true;
                            } else {
                                isValidRow = false;
                            }
                            temp = booleanArray[j];
                        }
                        if (andOrArray.length > 1 && booleanArray[j + 1] != null) {
                            if (booleanArray[j].toUpperCase().equalsIgnoreCase("TRUE") || temp.toUpperCase().equalsIgnoreCase("TRUE")) {
                                isValidRow = true;
                            } else {
                                isValidRow = false;
                            }
                            temp = booleanArray[j];
                        }
                    }
                }
            }
        }
        else{
            for (int j = 0; j < booleanArray.length; j++) {

                if (booleanArray[j].toUpperCase().equalsIgnoreCase("TRUE")) {
                    isValidRow = true;
                } else {
                    isValidRow = false;
                }
            }
        }
        return isValidRow;
    }



    public static void grabInformationFromWhereClause(String tableName, DatabaseManager databaseManager, String[] attributes, String[] conditions) {
        //this method grabs the information from the conditional where clause, like the relationalOp, attributeToCheck, recordToLocate, andORArray
        String path = "databases" + File.separator + databaseManager.getCurrentDatabase() + File.separator + tableName.toLowerCase() + "Records.txt";
        String line;
        String[] headerAttributes;
        int index;
        DatabaseManager dbManager = databaseManager;
        String conditionsString = String.join(" ", conditions);
        String[] andORArray = new String[findSizeOfAndOrArray(conditions)];
        Boolean containsAnd = false;
        Boolean containsOr = false;
        Boolean containsOrAnd = false;

        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            line = reader.readLine().trim();
            headerAttributes = line.split("\\s+");
            index = 0;
            int size = 0;
            for(int i = 0; i < headerAttributes.length; i++){
                for(int j = 0; j < conditions.length; j++){
                    if(headerAttributes[i].equalsIgnoreCase(conditions[j])){
                        size++;
                    }
                }
            }
            String[] attributeToCheck = new String[size];
            String[] recordToLocate = new String[size];
            String[] relationalOp = new String[size];
            for(int i = 0; i < conditions.length; i++){
                for(int j = 0; j < headerAttributes.length; j++){
                    if(conditions[i].equalsIgnoreCase(headerAttributes[j])){
                        if(conditions[i+1].equalsIgnoreCase("=")){
                            relationalOp[index] = conditions[i+1]; //store relational operator
                            attributeToCheck[index] = conditions[i]; //store the attributes to compare
                            recordToLocate[index] = conditions[i+2]; //record to locate
                            index++;
                        }
                        if(conditions[i+1].equalsIgnoreCase("!=")){
                            relationalOp[index] = conditions[i+1]; //store relational operator
                            attributeToCheck[index] = conditions[i]; //store the attributes to compare
                            recordToLocate[index] = conditions[i+2]; //record to locate
                            index++;
                        }
                        if(conditions[i+1].equalsIgnoreCase("<")){
                            relationalOp[index] = conditions[i+1]; //store relational operator
                            attributeToCheck[index] = conditions[i]; //store the attributes to compare
                            recordToLocate[index] = conditions[i+2]; //record to locate
                            index++;
                        }
                        if(conditions[i+1].equalsIgnoreCase(">")){
                            relationalOp[index] = conditions[i+1]; //store relational operator
                            attributeToCheck[index] = conditions[i]; //store the attributes to compare
                            recordToLocate[index] = conditions[i+2]; //record to locate
                            index++;
                        }
                        if(conditions[i+1].equalsIgnoreCase("<=")){
                            relationalOp[index] = conditions[i+1]; //store relational operator
                            attributeToCheck[index] = conditions[i]; //store the attributes to compare
                            recordToLocate[index] = conditions[i+2]; //record to locate
                            index++;
                        }
                        if(conditions[i+1].equalsIgnoreCase(">=")){
                            relationalOp[index] = conditions[i+1]; //store relational operator
                            attributeToCheck[index] = conditions[i]; //store the attributes to compare
                            recordToLocate[index] = conditions[i+2]; //record to locate
                            index++;
                        }
                    }
                }

            }
            //boolean checker to see if the condition where statement contains and or or
            if(conditionsString.toUpperCase().contains("AND")){
                containsAnd = true;
            }
            else if(conditionsString.toUpperCase().contains("OR")){
                containsOr = true;
            }
            else if(conditionsString.toUpperCase().contains("AND") && conditionsString.toUpperCase().contains("OR")){
                containsOrAnd = true;
            }
            //if it does contain it we loop through the condition statement and store the and or or
            if(containsAnd || containsOr || containsOrAnd){
                index = 0;
                for(int i = 0; i < conditions.length; i++){
                    if(conditions[i].equalsIgnoreCase("AND") || conditions[i].equalsIgnoreCase("OR")){
                        andORArray[index] = conditions[i];
                        index++;
                    }
                }
            }
            //pass the selected attributes to compare, the values, and the relational operator and pass to a select method
            reader.close();
            compareRows(tableName, dbManager, attributes, relationalOp, attributeToCheck, recordToLocate, andORArray);

        } catch (FileNotFoundException e) {
            System.out.println("No database Selected.");
            return;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static int findSizeOfAndOrArray(String[] string){
        int size = 0;
        for(int i = 0; i < string.length; i++){
            if(string[i].equalsIgnoreCase("AND") || string[i].equalsIgnoreCase("OR")){
                size++;
            }
        }
        return size;
    }


    public int findSizeOfConditions(DatabaseManager databaseManager, String tableName, String[] conditions) throws IOException {
        String path = "databases" + File.separator + databaseManager.getCurrentDatabase() + File.separator + tableName.toLowerCase() + "Records.txt";
        int index;
        String line;
        String[] headerAttributes;
        BufferedReader reader = new BufferedReader(new FileReader(path));
        line = reader.readLine().trim();
        headerAttributes = line.split("\\s+");
        index = 0;
        int size = 0;
        for(int i = 0; i < headerAttributes.length; i++){
            for(int j = 0; j < conditions.length; j++){
                if(headerAttributes[i].equalsIgnoreCase(conditions[j])){
                    size++;
                }
            }
        }
        reader.close();
        return size;
    }
    public static int[] getSelectedAttributesIndex(String[] attributesSelected, DatabaseManager databaseManager, String tableName) {
        String path = "databases" + File.separator + databaseManager.getCurrentDatabase() + File.separator + tableName.toLowerCase() + "Records.txt";
        String line;
        String[] dbRow;
        int[] indices;

        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));

            line = reader.readLine().trim();
            line = line.replace("\"", "").trim();
            dbRow = line.split("\\s+");
            int index = 0;
            indices = new int[attributesSelected.length];
            for (int j = 0; j < attributesSelected.length; j++) {
                for (int i = 0; i < dbRow.length; i++) {
                    if (attributesSelected[j].equalsIgnoreCase(dbRow[i])) {
                        indices[index] = i;
                        index++;
                    }
                }
            }
            reader.close();
            return indices;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
