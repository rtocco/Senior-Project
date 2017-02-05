package simpledb.server;

import simpledb.remote.*;
import simpledb.buffer.*;
import java.rmi.registry.*;

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

      // Run unit tests.
      runBufferTests();

      // Kill server.
      System.exit(1);
   }

   static void runBufferTests() {
       BufferManagerTests.runTests();
   }
}
