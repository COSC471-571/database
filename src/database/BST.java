package database;

import java.io.*;
import java.util.*;


public class BST{
    
    private BSTNode root;
    private final String indexFile;
    private String tableName;
    private String primaryKey;
    
    public BST() {
        this.indexFile = "default.txt";
        this.root = null;
    }
    
    public BST(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;  // Initialize the primary key correctly
        this.indexFile = "databases/" + tableName + ".txt";
        this.root = loadTreeFromFile();    
    }
  
    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    private BSTNode insertRecursive(BSTNode node, int key, long recordPointer)
    {
        if(node == null)
            return new BSTNode(key, recordPointer);
        
        if(key < node.key)
            node.left = insertRecursive(node.left, key, recordPointer);
        else if(key > node.key)
            node.right = insertRecursive(node.right, key, recordPointer);
        return node;
    }
    
    public void insert(int key, long recordPointer) {
        root = insertRecursive(root, key, recordPointer);
        saveTreeToFile();
    }
    
    public BSTNode search(int key) {
        return searchRecursive(root, key);
    }
    
    private BSTNode searchRecursive(BSTNode node, int key)
    {
        if(node == null || node.key == key)
            return node;
        if(key < node.key)
            return searchRecursive(node.left, key);
        else
            return searchRecursive(node.right, key);
    }
    
    public void inOrderTraversal(BSTNode node)
    {
        if(node != null) {
            inOrderTraversal(node.left);
            System.out.println("Key: " + node.key + ", Record Pointer: " + node.recordPointer);
            inOrderTraversal(node.right);
        }
    }
    
    public void printTree() {
        inOrderTraversal(root);
    }
    
    public void saveTreeToFile() {
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(indexFile))){
            writeTreeToFile(root, writer);
        } catch (IOException e) {
            System.out.println("Error saving BST index: " + e.getMessage());
        }
    }
    
 // Helper method to traverse the tree and write each node to the file
    private void writeTreeToFile(BSTNode node, BufferedWriter writer) throws IOException {
        if (node != null) {
            writer.write(node.key + "," + node.recordPointer); // Write the key and record pointer
            writer.newLine(); // Write a new line after each node's data
            writeTreeToFile(node.left, writer); // Traverse the left subtree
            writeTreeToFile(node.right, writer); // Traverse the right subtree
        }
    }
    
    private BSTNode loadTreeFromFile() {
        File file = new File(indexFile);
        if(!file.exists())
            return null;
        
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            BSTNode rootNode = null;
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(",");
                int key = Integer.parseInt(data[0]);
                long recordPointer = Long.parseLong(data[1]);
                rootNode = insertRecursive(rootNode, key, recordPointer); // Insert nodes into the tree
            }
            return rootNode; 
        } catch (IOException e) {
            System.out.println("Error loading BST index: " + e.getMessage());
            return null; // Return null if an error occurs
        }
    }
    public long getRecordPointer(String key) {
        BSTNode node = search(Integer.parseInt(key));
        if (node != null) {
            return node.getRecordPointer();
        }
        return -1;
    }
    
}