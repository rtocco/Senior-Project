import simpledb.remote.*;
import simpledb.buffer.*;
import simpledb.server.*;
import simpledb.tx.*;
import simpledb.parse.*;
import java.rmi.registry.*;

import java.sql.*;
import java.util.ArrayList;
import simpledb.remote.SimpleDriver;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class Tests {
   public static void main(String args[]) throws Exception {
      // Configure and initialize the database.
      SimpleDB.init(args[0]);

      // Create a registry specific for the server on the default port.
      Registry reg = LocateRegistry.createRegistry(1099);

      // Post the server entry in it.
      RemoteDriver d = new RemoteDriverImpl();
      reg.rebind("simpledb", d);

      System.out.println("database server ready");

      setUpDB();

      // Run unit tests.
      // runBufferTests();
      // runTransactionTests();

      addToDB();
      runParserTests();

      // Kill server.
      System.exit(1);
   }

   // Set up the database for unit testing.
   static void setUpDB() {
      Connection connection = null;

      try {
          // Connect to the database server.
          Driver driver = new SimpleDriver();
          connection = driver.connect("jdbc:simpledb://localhost", null);
          Statement statement = connection.createStatement();

          // Delete the student table so we can start with a blank slate.
          String sql = "delete from student(id, year, make, model)";
          statement.executeUpdate(sql);

          // Create a table called student.
          sql = "create table student(id int, name varchar(15), age int)";
          statement.executeUpdate(sql);

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

   static void addToDB() {
      Connection connection = null;

      try {
          // Connect to the database server.
          Driver driver = new SimpleDriver();
          connection = driver.connect("jdbc:simpledb://localhost", null);
          Statement statement = connection.createStatement();

          // Delete the hotel and guest tables so we can start with a blank slate.
          String sql = "delete from hotels(hotelID, hotelName, price)";
          statement.executeUpdate(sql);

          sql = "delete from guests(guestID, guestName, age, hotel)";
          statement.executeUpdate(sql);


          // Create tables called hotel and guest
          sql = "create table hotels(hotelID int, hotelName varchar(15), price int)";
          statement.executeUpdate(sql);

          sql = "create table guests(guestID int, guestName varchar(15), age int, hotel varchar(15))";
          statement.executeUpdate(sql);


          // Insert records into hotels.
          sql = "insert into hotels(hotelID, hotelName, price) values(1, 'Marriot', 100)";
          statement.executeUpdate(sql);

          sql = "insert into hotels(hotelID, hotelName, price) values(2, 'Hilton', 150)";
          statement.executeUpdate(sql);

          sql = "insert into hotels(hotelID, hotelName, price) values(3, 'Holiday Inn', 100)";
          statement.executeUpdate(sql);

          // Insert records into guests.
          sql = "insert into guests(guestID, guestName, age, hotel) values(1, 'Matthew', 20, 'Marriot')";
          statement.executeUpdate(sql);

          sql = "insert into guests(guestID, guestName, age, hotel) values(2, 'Mark', 20, 'Hilton')";
          statement.executeUpdate(sql);

          sql = "insert into guests(guestID, guestName, age, hotel) values(3, 'Luke', 21, 'Holiday Inn')";
          statement.executeUpdate(sql);

          sql = "insert into guests(guestID, guestName, age, hotel) values(4, 'John', 22, 'Marriot')";
          statement.executeUpdate(sql);

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

   // Run tests on the buffer manager.
   static void runBufferTests() {
      Result result = JUnitCore.runClasses(BufferManagerTests.class);

      System.out.println("\n");

      for (Failure failure : result.getFailures()) {
         System.out.println(failure.toString());
      }

      System.out.println("\n");

      if(result.wasSuccessful()) {
         System.out.println("All buffer tests passed");
      } else {
         System.out.println("At least one buffer test failed");
      }
   }

   // Run tests on transactions and the recovery manager.
   static void runTransactionTests() {
      Result result = JUnitCore.runClasses(TransactionTests.class);

      System.out.println("\n");

      for (Failure failure : result.getFailures()) {
         System.out.println(failure.toString());
      }

      System.out.println("\n");

      if(result.wasSuccessful()) {
         System.out.println("All transaction tests passed");
      } else {
         System.out.println("At least one transaction test failed");
      }
   }

   // Run tests on the parser
   static void runParserTests() {
      Result result = JUnitCore.runClasses(ParserTests.class);

      System.out.println("\n");

      for (Failure failure : result.getFailures()) {
         System.out.println(failure.toString());
      }

      System.out.println("\n");

      if(result.wasSuccessful()) {
         System.out.println("All parser tests passed");
      } else {
         System.out.println("At least one parser test failed");
      }
   }
}
