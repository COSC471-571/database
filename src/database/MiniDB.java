package database;

import java.util.Scanner;

public class MiniDB {

    public static void main(String[] args) {
       
        Scanner scanner = new Scanner(System.in);
        
        DatabaseManager dbManager = new DatabaseManager();
        
        System.out.println("Welcome to Mini DB. Type EXIT to quit");
        
        while(true) {
            if (dbManager.getCurrentDatabase() == null) {
                System.out.print("MiniDB > ");
            }else {
                System.out.print("MiniDB [" + dbManager.getCurrentDatabase() + "]> ");
            }

            String command = scanner.nextLine().trim();
            
            if(command.equalsIgnoreCase("EXIT")) {
                System.out.println("Exiting MiniDB...");
                break;
            }
            
           Parser.parse(command, dbManager);
        }
        
      scanner.close();

    }
   

}
