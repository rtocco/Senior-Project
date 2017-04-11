// This program can be run to execute any desired SQL statement from the
// command line. It must be updated as new SQL commands are implemented.

import java.sql.*;
import simpledb.remote.SimpleDriver;
import java.util.Scanner;

public class CommandLineTests {
    static Connection connection;
    static Driver driver;
    static Statement statement;


    public static void main(String[] args) {

        connection = null;

        try {
            // Connect to the database server.
            driver = new SimpleDriver();
            connection = driver.connect("jdbc:simpledb://localhost", null);
            statement = connection.createStatement();

            // Set up user input.
            Scanner scanner = new Scanner(System.in);
            String input = "";
            String firstWord = "";

            while(true) {
                System.out.print("SQL> ");

                input = scanner.nextLine();
                firstWord = input.split(" ")[0].toLowerCase();

                if(firstWord.equals("exit")) {
                    break;
                } else if(firstWord.equals("select")) {
                    query(input);
                } else {
                    update(input);
                }
            }

        } catch(SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if(connection != null) { connection.close(); }
            } catch(SQLException e) {
                e.printStackTrace();
            }
        }
    }

    static void query(String input) {
      try {
        ResultSet resultSet = statement.executeQuery(input);
        ResultSetMetaData metaData = resultSet.getMetaData();

        for(int i = 1; i <= metaData.getColumnCount(); i++) {
            System.out.print(metaData.getColumnName(i) + "\t");
        }
        System.out.println();

        while(resultSet.next()) {
            for(int i = 1; i <= metaData.getColumnCount(); i++) {
                if(metaData.getColumnType(i) == Types.VARCHAR || metaData.getColumnType(i) == Types.CHAR) {
                    System.out.print(resultSet.getString(metaData.getColumnName(i)) + "\t\t");
                } else {
                    System.out.print(resultSet.getInt(metaData.getColumnName(i)) + "\t\t");
                }
            }
            System.out.println("\n\n");
        }
      } catch(SQLException e) {
        System.out.println(e.toString());
      }
    }

    static void update(String input) {
      try {
        statement.executeUpdate(input);
      } catch(SQLException e) {
        System.out.println(e.toString());
      }
    }
}
