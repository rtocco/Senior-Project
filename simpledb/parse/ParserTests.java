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
      System.out.println("\nOriginal Functionality 1");
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
         e.printStackTrace();
         assertTrue("Original 1: Should not throw an exception.", false);
      }

      System.out.println("\nOriginal Functionality 2");
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
         e.printStackTrace();
         assertTrue("Original 2: Should not throw an exception.", false);
      }
   }

   // Test whether the or term functionality works properly.
   @Test
   public void orTerm() {
      System.out.println("\nOr Term Test 1");
      try {
         Parser parser = new Parser("select hotelName, price from hotels where hotelName='Marriot' or hotelName='Hilton'");
         QueryData data = parser.query();
         QueryPlanner planner = new BasicQueryPlanner();
         Transaction tx = new Transaction();
         Plan plan = planner.createPlan(data, tx);
         Scan scan = plan.open();

         ArrayList<String> hotelNames = new ArrayList<String>();
         ArrayList<Integer> prices = new ArrayList<Integer>();
         while(scan.next()) {
            hotelNames.add(scan.getString("hotelname"));
            prices.add(scan.getInt("price"));
         }
         assertTrue("Marriot should be in results", hotelNames.contains("Marriot"));
         assertTrue("Hilton should be in results", hotelNames.contains("Hilton"));
         assertTrue("Price of 100 should be in the results", prices.contains(100));
         assertTrue("Price of 150 should be in the results", prices.contains(150));

         assertTrue("Holiday Inn should not be in results", !hotelNames.contains("Holiday Inn"));

      } catch(Exception e) {
         e.printStackTrace();
         assertTrue("Or: Should not throw an exception.", false);
      }

      System.out.println("\nOr Term Test 2");
      boolean exceptionThrown = false;
      try {
         Parser parser = new Parser("select hotelName from hotels where hotelName='Marriot' or hotelName='Hilton' and price=150");
         QueryData data = parser.query();
      } catch(BadSyntaxException e) {
         exceptionThrown = true;
      }
      assertTrue("Or: Should throw a Bad Syntax Exception.", exceptionThrown);
   }

   @Test
   public void starField() {
      System.out.println("\nStar Field Test");
      try {
         Parser parser = new Parser("select * from hotels");
         QueryData data = parser.query();
         QueryPlanner planner = new BasicQueryPlanner();
         Transaction tx = new Transaction();
         Plan plan = planner.createPlan(data, tx);
         Scan scan = plan.open();

         ArrayList<Integer> hotelIDs = new ArrayList<Integer>();
         ArrayList<String> hotelNames = new ArrayList<String>();
         ArrayList<Integer> prices = new ArrayList<Integer>();
         while(scan.next()) {
            hotelIDs.add(scan.getInt("hotelid"));
            hotelNames.add(scan.getString("hotelname"));
            prices.add(scan.getInt("price"));
         }
         assertTrue("ID 1 should be in results", hotelIDs.contains(1));
         assertTrue("ID 2 should be in results", hotelIDs.contains(2));
         assertTrue("ID 3 should be in results", hotelIDs.contains(3));
         assertTrue("Marriot should be in results", hotelNames.contains("Marriot"));
         assertTrue("Hilton should be in results", hotelNames.contains("Hilton"));
         assertTrue("Holiday Inn should be in results", hotelNames.contains("Holiday Inn"));
         assertTrue("Price of 100 should be in the results", prices.contains(100));
         assertTrue("Price of 150 should be in the results", prices.contains(150));

      } catch(Exception e) {
         e.printStackTrace();
         assertTrue("Or: Should not throw an exception.", false);
      }
   }

   @Test
   public void groupBy() {
      System.out.println("\nGroup By Test");
      try {
         Parser parser = new Parser("select price from hotels group by price");
         QueryData data = parser.query();
         QueryPlanner planner = new BasicQueryPlanner();
         Transaction tx = new Transaction();
         Plan plan = planner.createPlan(data, tx);
         Scan scan = plan.open();

         ArrayList<Integer> prices = new ArrayList<Integer>();
         while(scan.next()) {
            prices.add(scan.getInt("price"));
         }
         assertTrue("Price of 150 should be in the results", prices.contains(150));
         int num = 0;
         for(int i = 0; i < prices.size(); i++) {
            if(prices.get(i) == 100) num++;
         }
         assertEquals("Should only contain 1 price of 100", num, 1);

      } catch(Exception e) {
         e.printStackTrace();
         assertTrue("Or: Should not throw an exception.", false);
      }
   }
}
