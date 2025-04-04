package database;

public class BSTNode {
    
    int key;
    long recordPointer;
    BSTNode left, right;
    
    public BSTNode(int key, long recordPointer)
    {
        this.key = key;
        this.recordPointer = recordPointer;
        this.left = this.right = null;
    }
    public long getRecordPointer() {
        return this.recordPointer;
    }
}