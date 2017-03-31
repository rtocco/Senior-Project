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
         tx.commit();

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
         tx.commit();

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
         tx.commit();

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
         tx.commit();

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
         assertTrue("Star Field: Should not throw an exception.", false);
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
         tx.commit();

         assertEquals("Should only contain 1 price of 100", num, 1);

      } catch(Exception e) {
         e.printStackTrace();
         assertTrue("Group By: Should not throw an exception.", false);
      }
   }

   @Test
   public void countFunction() {
      System.out.println("\nCount Function Test");
      try {
         Parser parser = new Parser("select price, count(price) from hotels group by price");
         QueryData data = parser.query();
         QueryPlanner planner = new BasicQueryPlanner();
         Transaction tx = new Transaction();
         Plan plan = planner.createPlan(data, tx);
         Scan scan = plan.open();

         ArrayList<Integer> prices = new ArrayList<Integer>();
         int price100 = 0;
         int price150 = 0;
         while(scan.next()) {
            prices.add(scan.getInt("price"));
            if(scan.getInt("price") == 100) {
               price100 = scan.getInt("countofprice");
            } else if(scan.getInt("price") == 150) {
               price150 = scan.getInt("countofprice");
            }
         }
         tx.commit();

         assertTrue("Price of 150 should be in the results", prices.contains(150));
         assertEquals("count(100) should be 2", price100, 2);
         assertEquals("count(150) should be 1", price150, 1);

      } catch(Exception e) {
         e.printStackTrace();
         assertTrue("Count: Should not throw an exception.", false);
      }
   }

   @Test
   public void maxFunction() {
      System.out.println("\nMax Function Test");
      try {
         Parser parser = new Parser("select hotel, max(age) from guests group by hotel");
         QueryData data = parser.query();
         QueryPlanner planner = new BasicQueryPlanner();
         Transaction tx = new Transaction();
         Plan plan = planner.createPlan(data, tx);
         Scan scan = plan.open();

         ArrayList<String> hotels = new ArrayList<String>();
         int marriotAge = 0;
         int hiltonAge = 0;
         int holidayInnAge = 0;
         while(scan.next()) {
            hotels.add(scan.getString("hotel"));
            if(scan.getString("hotel").equals("Marriot")) {
               marriotAge = scan.getInt("maxofage");
            } else if(scan.getString("hotel").equals("Hilton")) {
               hiltonAge = scan.getInt("maxofage");
            } else if(scan.getString("hotel").equals("Holiday Inn")) {
               holidayInnAge = scan.getInt("maxofage");
            }
         }
         tx.commit();

         assertEquals("The max age of a Marriot guest should be 22", marriotAge, 22);
         assertEquals("The max age of a Hilton guest should be 20", hiltonAge, 20);
         assertEquals("The max age of a Holiday Inn guest should be 21", holidayInnAge, 21);

      } catch(Exception e) {
         e.printStackTrace();
         assertTrue("Max: Should not throw an exception.", false);
      }
   }

   @Test
   public void minFunction() {
      System.out.println("\nMin Function Test");
      try {
         Parser parser = new Parser("select hotel, min(age) from guests group by hotel");
         QueryData data = parser.query();
         QueryPlanner planner = new BasicQueryPlanner();
         Transaction tx = new Transaction();
         Plan plan = planner.createPlan(data, tx);
         Scan scan = plan.open();

         ArrayList<String> hotels = new ArrayList<String>();
         int marriotAge = 0;
         int hiltonAge = 0;
         int holidayInnAge = 0;
         while(scan.next()) {
            hotels.add(scan.getString("hotel"));
            if(scan.getString("hotel").equals("Marriot")) {
               marriotAge = scan.getInt("minofage");
            } else if(scan.getString("hotel").equals("Hilton")) {
               hiltonAge = scan.getInt("minofage");
            } else if(scan.getString("hotel").equals("Holiday Inn")) {
               holidayInnAge = scan.getInt("minofage");
            }
         }
         tx.commit();

         assertEquals("The min age of a Marriot guest should be 20", marriotAge, 20);
         assertEquals("The min age of a Hilton guest should be 20", hiltonAge, 20);
         assertEquals("The min age of a Holiday Inn guest should be 21", holidayInnAge, 21);

      } catch(Exception e) {
         e.printStackTrace();
         assertTrue("Min: Should not throw an exception.", false);
      }
   }

   @Test
   public void sumFunction() {
      System.out.println("\nSum Function Test");
      try {
         Parser parser = new Parser("select hotel, sum(age) from guests group by hotel");
         QueryData data = parser.query();
         QueryPlanner planner = new BasicQueryPlanner();
         Transaction tx = new Transaction();
         Plan plan = planner.createPlan(data, tx);
         Scan scan = plan.open();

         ArrayList<String> hotels = new ArrayList<String>();
         int marriotSum = 0;
         int hiltonSum = 0;
         int holidayInnSum = 0;
         while(scan.next()) {
            hotels.add(scan.getString("hotel"));
            if(scan.getString("hotel").equals("Marriot")) {
               marriotSum = scan.getInt("sumofage");
            } else if(scan.getString("hotel").equals("Hilton")) {
               hiltonSum = scan.getInt("sumofage");
            } else if(scan.getString("hotel").equals("Holiday Inn")) {
               holidayInnSum = scan.getInt("sumofage");
            }
         }
         tx.commit();

         assertEquals("The sum of Marriot guest ages should be 42", marriotSum, 42);
         assertEquals("The sum of Hilton guest ages should be 20", hiltonSum, 20);
         assertEquals("The sum of Holiday Inn guest ages should be 21", holidayInnSum, 21);

      } catch(Exception e) {
         e.printStackTrace();
         assertTrue("Sum: Should not throw an exception.", false);
      }
   }

   @Test
   public void avgFunction() {
      System.out.println("\nAvg Function Test");
      try {
         Parser parser = new Parser("select hotel, avg(age) from guests group by hotel");
         QueryData data = parser.query();
         QueryPlanner planner = new BasicQueryPlanner();
         Transaction tx = new Transaction();
         Plan plan = planner.createPlan(data, tx);
         Scan scan = plan.open();

         ArrayList<String> hotels = new ArrayList<String>();
         int marriotAvg = 0;
         int hiltonAvg = 0;
         int holidayInnAvg = 0;
         while(scan.next()) {
            hotels.add(scan.getString("hotel"));
            if(scan.getString("hotel").equals("Marriot")) {
               marriotAvg = scan.getInt("avgofage");
            } else if(scan.getString("hotel").equals("Hilton")) {
               hiltonAvg = scan.getInt("avgofage");
            } else if(scan.getString("hotel").equals("Holiday Inn")) {
               holidayInnAvg = scan.getInt("avgofage");
            }
         }
         tx.commit();

         assertEquals("The avg of Marriot guest ages should be 21", marriotAvg, 21);
         assertEquals("The avg of Hilton guest ages should be 20", hiltonAvg, 20);
         assertEquals("The avg of Holiday Inn guest ages should be 21", holidayInnAvg, 21);

      } catch(Exception e) {
         e.printStackTrace();
         assertTrue("Avg: Should not throw an exception.", false);
      }
   }

   @Test
   public void having() {
      System.out.println("\nHaving Test");
      try {
         Parser parser = new Parser("select hotel, avg(age) from guests group by hotel having avg(age)=21 and sum(age)=42");
         QueryData data = parser.query();
         QueryPlanner planner = new BasicQueryPlanner();
         Transaction tx = new Transaction();
         Plan plan = planner.createPlan(data, tx);
         Scan scan = plan.open();

         ArrayList<String> hotels = new ArrayList<String>();
         ArrayList<Integer> averageAges = new ArrayList<Integer>();
         while(scan.next()) {
            hotels.add(scan.getString("hotel"));
            averageAges.add(scan.getInt("avgofage"));
         }
         tx.commit();

         assertTrue("Marriot should be in hotels.", hotels.contains("Marriot"));
         assertTrue("Hilton should not be in hotels", !hotels.contains("Hilton"));
         assertTrue("Holiday Inn should not be in hotels", !hotels.contains("Holiday Inn"));

         assertTrue("21 should be an average age", averageAges.contains(21));
      } catch(Exception e) {
         e.printStackTrace();
         assertTrue("Having: Should not throw an exception.", false);
      }
   }
}
