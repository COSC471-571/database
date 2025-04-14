package database;

import java.io.*;
import java.util.*;

import static java.util.Objects.isNull;

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
                        case "text":
                            if (!charConstraintCheck(value.trim())) {
                                return false;
                            }
                            break;
                        case "string":
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
                    values[count] = part[index - 1];
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

    public static void selectAttributeRow(String tableName, DatabaseManager databaseManager, String[] storedRow, String[] selectedAttributes) {
        //select and print the rows that satisfy the condition
        String path = "databases" + File.separator + databaseManager.getCurrentDatabase() + File.separator + tableName.toLowerCase() + "Records.txt";
        String line;
        int rowCount = 0;
        StringBuilder attributeList = new StringBuilder();
        int columnWidth = 20;
        StringBuilder lineData;
        String[] dbRow;
        String removedQuotesLine = "";

        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            //find the index of the listed attributes
            line = reader.readLine().trim();
            dbRow = line.split("\\s+");
            int index = 0;
            int[] indices = new int[selectedAttributes.length];

            for (int i = 0; i < dbRow.length; i++) {
                for (int j = 0; j < selectedAttributes.length; j++) {
                    selectedAttributes[j] = selectedAttributes[j].replace(",", "");
                    if (dbRow[i].equalsIgnoreCase(selectedAttributes[j])) {
                        indices[index] = i;
                        index++;
                        attributeList.append(String.format("%-" + columnWidth + "s", selectedAttributes[j]));
                    }
                }
            }
            System.out.println(attributeList.toString());

            while ((line = reader.readLine()) != null) {
                lineData = new StringBuilder();
                removedQuotesLine = line.replace("\"", "").trim();
                dbRow = removedQuotesLine.split("\\s+");
                for(int j = 0; j < storedRow.length;j++){
                    if(rowCount == Integer.parseInt(storedRow[j])){
                        for(int k = 0; k < indices.length; k++){
                            lineData.append(String.format("%-" + columnWidth + "s", dbRow[indices[k]]));
                        }
                        System.out.println(lineData.toString());
                    }
                }

                rowCount++;
            }
            System.out.println();
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
    private static void whereClauseAttribute(String tableName, DatabaseManager databaseManager, String[] attributesSelected, String[] relationalOp, String[] attributesFromWhereClause, String[] recordToLocate, String[] andOrArray) {
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
            System.out.println(String.format("%-" + columnWidth + "s", line));
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
            //get the index of the selcted attributes in the header
            for (int j = 0; j < attributesSelected.length; j++) {
                attributesSelected[j] = attributesSelected[j].replace(",", "").trim();
                for (int i = 0; i < dbRow.length; i++) {
                    if (attributesSelected[j].equalsIgnoreCase(dbRow[i])) {
                        headerIndex[index] = i;
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
                if (isValidRow(andOrArray, booleanArray)) {
                    for(int i = 0; i < headerIndex.length; i++){
                        lineData.append(String.format("%-" + columnWidth + "s", dbRow[headerIndex[i]]));
                        //System.out.print(String.format("%-" + columnWidth + "s", dbRow[headerIndex[i]]));
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
    public void returnHeaderIndex(){

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


    public static void conditionalSelection(String tableName, DatabaseManager databaseManager, String[] attributes, String[] conditions) {
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
            whereClauseAttribute(tableName, dbManager, attributes,relationalOp,attributeToCheck,recordToLocate,andORArray);

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

    public static void handleDelete(String command, String dbManager) {
        command = command.replace(";", "").trim();
        String[] parts = command.split("\\s+");
        if (parts.length < 2) {
            System.out.println("Invalid DELETE syntax.");
            return;
        }

        String tableName = parts[1].trim();
        String conditionAttr = null;
        String conditionVal = null;

        if (command.toUpperCase().contains("WHERE")) {
            int whereIndex = command.toUpperCase().indexOf("WHERE");
            String condition = command.substring(whereIndex + 5).trim();
            String[] condParts = condition.split("=");
            if (condParts.length == 2) {
                conditionAttr = condParts[0].trim();
                conditionVal = condParts[1].trim().replaceAll("\"", "");
            } else {
                System.out.println("Invalid WHERE clause.");
                return;
            }
        }

        deleteRecords(tableName, dbManager, conditionAttr, conditionVal);
    }
    public static void deleteRecords(String tableName, String dbManager, String condAttr, String condVal) {
        String basePath = "databases" + File.separator + dbManager.toLowerCase() + File.separator;
        File recordFile = new File(basePath + tableName.toLowerCase() + "Records.txt");

        if (!recordFile.exists()) {
            System.out.println("Table does not exist.");
            return;
        }

        if (condAttr == null || condVal == null) {
            // Delete entire table
            recordFile.delete();
            new File(basePath + tableName.toLowerCase() + "Attribute.txt").delete();
            new File(basePath + tableName.toLowerCase() + "Index.txt").delete();
            System.out.println("All records and schema for table " + tableName + " deleted.");
            return;
        }

        // Conditional delete
        try {
            BufferedReader reader = new BufferedReader(new FileReader(recordFile));
            List<String> lines = new ArrayList<>();
            String headerLine = reader.readLine();
            lines.add(headerLine); // always keep the header

            String[] headers = headerLine.trim().split("\\s+");
            int condIndex = -1;
            for (int i = 0; i < headers.length; i++) {
                if (headers[i].equalsIgnoreCase(condAttr)) {
                    condIndex = i;
                    break;
                }
            }

            if (condIndex == -1) {
                System.out.println("Attribute " + condAttr + " not found.");
                return;
            }

            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.trim().split("\\s+");
                if (!values[condIndex].equals(condVal)) {
                    lines.add(line); // keep lines not matching
                }
            }
            reader.close();

            BufferedWriter writer = new BufferedWriter(new FileWriter(recordFile));
            for (String l : lines) {
                writer.write(l);
                writer.newLine();
            }
            writer.close();

            System.out.println("Records deleted where " + condAttr + " = " + condVal);

        } catch (IOException e) {
            System.out.println("Error deleting records: " + e.getMessage());
        }
    }
    public static void updateRecords(String tableName, String dbManager, Map<String, String> setMap, String condAttr, String condVal) {
        String path = "databases" + File.separator + dbManager + File.separator + tableName.toLowerCase() + "Records.txt";
        File file = new File(path);
        if (!file.exists()) {
            System.out.println("Table does not exist.");
            return;
        }

        try {
            List<String> lines = new ArrayList<>();
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String headerLine = reader.readLine();
            lines.add(headerLine);

            String[] headers = headerLine.trim().split("\\s+");
            int condIndex = -1;
            if (condAttr != null) {
                for (int i = 0; i < headers.length; i++) {
                    if (headers[i].equalsIgnoreCase(condAttr)) {
                        condIndex = i;
                        break;
                    }
                }
            }

            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.trim().split("\\s+");
                boolean matches = true;

                if (condAttr != null && condIndex != -1) {
                    matches = values[condIndex].equals(condVal);
                }

                if (matches) {
                    for (int i = 0; i < headers.length; i++) {
                        if (setMap.containsKey(headers[i])) {
                            values[i] = setMap.get(headers[i]);
                        }
                    }
                }

                StringBuilder updatedLine = new StringBuilder();
                for (String v : values) {
                    updatedLine.append(String.format("%-20s", v));
                }
                lines.add(updatedLine.toString());
            }

            reader.close();

            BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            for (String updated : lines) {
                writer.write(updated);
                writer.newLine();
            }
            writer.close();
            System.out.println("Update successful.");

        } catch (IOException e) {
            System.out.println("Error updating records: " + e.getMessage());
        }
    }
    public static void handleUpdate(String command, String dbManager) {
        command = command.replace(";", "");
        String[] updateParts = command.split("SET", 2);
        if (updateParts.length != 2) {
            System.out.println("Invalid UPDATE syntax.");
            return;
        }

        String tableName = updateParts[0].split("\\s+")[1].trim();
        String setAndWhere = updateParts[1].trim();
        String[] setParts = setAndWhere.split("WHERE", 2);
        String setClause = setParts[0].trim();

        Map<String, String> setMap = new HashMap<>();
        for (String pair : setClause.split(",")) {
            String[] kv = pair.trim().split("=", 2);
            if (kv.length == 2) {
                setMap.put(kv[0].trim(), kv[1].trim().replaceAll("\"", ""));
            }
        }

        String conditionAttr = null;
        String conditionValue = null;

        if (setParts.length == 2) {
            String[] condParts = setParts[1].trim().split("=", 2);
            if (condParts.length == 2) {
                conditionAttr = condParts[0].trim();
                conditionValue = condParts[1].trim().replaceAll("\"", "");
            }
        }

        updateRecords(tableName, dbManager, setMap, conditionAttr, conditionValue);
    }
}
