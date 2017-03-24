package simpledb.parse;

import java.util.*;

import simpledb.query.*;
import simpledb.planner.*;
import simpledb.record.*;
import simpledb.tx.*;
import static org.junit.Assert.*;
import org.junit.Test;

public class ParserTests {

   // Test to ensure the preexisting functionality is still working properly.
   @Test
   public void originalFunctionality() {
      try {
         Parser parser = new Parser("select hotelName from hotels");
         QueryData data = parser.query();
         Collection<String> fields = data.fields();
         Collection<String> tables = data.tables();

         assertTrue("Collection fields should contain hotelname", fields.contains("hotelname"));
         assertTrue("Collection tables should contain hotels", tables.contains("hotels"));

         QueryPlanner planner = new BasicQueryPlanner();
         Transaction tx = new Transaction();
         Plan plan = planner.createPlan(data, tx);
         Scan scan = plan.open();

         ArrayList<String> hotelNames = new ArrayList<String>();
         while(scan.next()) {
            hotelNames.add(scan.getString("hotelname"));
         }
         assertTrue("Marriot should be in results", hotelNames.contains("Marriot"));
         assertTrue("Hilton should be in results", hotelNames.contains("Hilton"));
         assertTrue("Holiday Inn should be in results", hotelNames.contains("Holiday Inn"));

      } catch(Exception e) {
         System.out.println(e.toString());
         assertTrue("Should not throw an exception.", false);
      }

      try {
         Parser parser = new Parser("select guestName, hotelName from guests, hotels where hotelName=hotel and age=20");
         QueryData data = parser.query();
         Collection<String> fields = data.fields();
         Collection<String> tables = data.tables();
         Predicate predicate = data.pred();

         assertTrue("Collection fields should contain hotelName", fields.contains("hotelname"));
         assertTrue("Collection fields should contain guestname", fields.contains("guestname"));
         assertTrue("Collection tables should contain hotels", tables.contains("hotels"));
         assertTrue("Collection tables should contain guests", tables.contains("guests"));
         assertEquals("Predicate should be correct", predicate.toString(), "hotelname=hotel and age=20");

         QueryPlanner planner = new BasicQueryPlanner();
         Transaction tx = new Transaction();
         Plan plan = planner.createPlan(data, tx);
         Scan scan = plan.open();

         ArrayList<String> hotelNames = new ArrayList<String>();
         ArrayList<String> guestNames = new ArrayList<String>();
         while(scan.next()) {
            hotelNames.add(scan.getString("hotelname"));
            guestNames.add(scan.getString("guestname"));
         }
         assertTrue("Marriot should be in the results", hotelNames.contains("Marriot"));
         assertTrue("Hilton should be in the results", hotelNames.contains("Hilton"));
         assertTrue("Matthew should be in the results", guestNames.contains("Matthew"));
         assertTrue("Mark should be in the results", guestNames.contains("Mark"));

         assertTrue("Holiday Inn should not be in the results", !hotelNames.contains("Holiday Inn"));
         assertTrue("Luke should not be in the results", !guestNames.contains("Luke"));
         assertTrue("John should not be in the results", !guestNames.contains("John"));

      } catch(Exception e) {
         System.out.println(e.toString());
         assertTrue("Should not throw an exception.", false);
      }
   }
}
