import simpledb.remote.*;
import simpledb.buffer.*;
import simpledb.server.*;
import java.rmi.registry.*;

import java.sql.*;
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
      runBufferTests();

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
          System.out.println(sql);
          statement.executeUpdate(sql);

          // Create a table called student.
          sql = "create table student(id int, name varchar(15), age int)";
          System.out.println(sql);
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
         System.out.println("All tests passed");
      } else {
         System.out.println("At least one test failed");
      }
   }
}
