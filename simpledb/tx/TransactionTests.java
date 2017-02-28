package simpledb.tx;

import static org.junit.Assert.*;
import org.junit.Test;
import java.util.Iterator;

import simpledb.file.Block;
import simpledb.record.*;
import simpledb.tx.recovery.*;
import simpledb.tx.concurrency.*;

public class TransactionTests {

   // Tests the transactions ability to set int and String values to a block.
   // Also tests whether these are properly added to the log file.
   @Test
   public void transactionTests() {
      System.out.println("\nTransaction Tests");

      Transaction transaction = new Transaction();
      Schema schema = new Schema();
      TableInfo tableInfo = new TableInfo("student", schema);
      RecordFormatter recordFormatter = new RecordFormatter(tableInfo);
      Block block = transaction.append("student.tbl", recordFormatter);
      transaction.pin(block);

      // This should put values in the block and add to the log file.
      transaction.setInt(block, 0, 20);
      transaction.setString(block, 4, "Test");

      // Test whether the values were properly added to the block.
      assertEquals("Block should have value 20 at offset 0.", transaction.getInt(block, 0), 20);
      assertEquals("Block should have value Test at offset 4", transaction.getString(block, 4), "Test");

      // Test whether the proper additions were made to the log file.
      Iterator<LogRecord> iter = new LogRecordIterator();
      LogRecord record = iter.next();
      assertEquals("Last entry should be of type SETSTRING", record.op(), 5);
      assertEquals("Last entry should have value correct value", record.toString(), "<SETSTRING " + transaction.getTxNum() + " " + block + " " + 4 + " >");

      record = iter.next();
      assertEquals("Second to last entry should be of type SETINT", record.op(), 4);
      assertEquals("Second to last entry should have the correct value", record.toString(), "<SETINT " + transaction.getTxNum() + " " + block + " 0 0>");
   }

   // Test whether the system can handle receiving reads and
   // writes one after the other from the same transaction.
   // @Test
   // public void concurrencyTest1() {
   //    Transaction transaction = new Transaction();
   //    Block block = new Block("student.tbl", 1);
   //    transaction.pin(block);
   //
   //    assertEquals("Value should initially be 0", transaction.getInt(block, 0), 0);
   //    transaction.setInt(block, 0, 5);
   //    assertEquals("Value should now be 5", transaction.getInt(block, 0), 5);
   //
   //    transaction.setInt(block, 0, 10);
   //    assertEquals("Value should now be 10", transaction.getInt(block, 0), 10);
   // }

   // Create deadlock using two processes, both writing to blocks.
   // @Test
   // public void concurrencyTest2() {
   //    System.out.println("\nTwo-transaction Concurrency Tests");
   //
   //    // Keep track of the time.
   //    long beginTime = System.currentTimeMillis();
   //
   //    // Start two transactions that will cause deadlock to occur.
   //    Thread thread1 = new Thread(new Test2Thread1("thread1"), "thread1");
   //    Thread thread2 = new Thread(new Test2Thread2("thread2"), "thread2");
   //    thread1.start();
   //    thread2.start();
   //
   //    try {
   //       thread1.join();
   //       thread2.join();
   //    } catch(InterruptedException e) {
   //       System.out.println(e.toString());
   //    }
   //
   //    long endTime = System.currentTimeMillis();
   //
   //    // Initial deadlock detection timed out after 5 seconds. New one should work quickly.
   //    assertTrue("Test 2: Deadlock should be detected in less than 3 seconds", (endTime - beginTime) < 3000);
   //
   //    // Test if thread 1 changed the values in blocks 1 and 2.
   //    Transaction transaction = new Transaction();
   //    Block block2 = new Block("student.tbl", 2);
   //    Block block3 = new Block("student.tbl", 3);
   //    transaction.pin(block2);
   //    transaction.pin(block3);
   //
   //    assertEquals("Block 2 should equal 2 at offset 0", transaction.getInt(block2, 0), 2);
   //    assertEquals("Block 3 should equal 3 at offset 0", transaction.getInt(block3, 0), 3);
   // }

   // Test deadlock with 3 transactions and a mixture of sLocks and xLocks
   // @Test
   // public void concurrencyTest3() {
   //    long beginTime = System.currentTimeMillis();
   //
   //    // Start four transactions that will cause deadlock to occur.
   //    Thread thread1 = new Thread(new Test3Thread1("thread1"), "thread1");
   //    Thread thread2 = new Thread(new Test3Thread2("thread2"), "thread2");
   //    Thread thread3 = new Thread(new Test3Thread3("thread3"), "thread3");
   //    thread1.start();
   //    thread2.start();
   //    thread3.start();
   //
   //    try {
   //       thread1.join();
   //       thread2.join();
   //       thread3.join();
   //    } catch(InterruptedException e) {
   //       System.out.println(e.toString());
   //    }
   //
   //    long endTime = System.currentTimeMillis();
   //
   //    // assertTrue("Test 3: Deadlock should be detected in less than 3 seconds", (endTime - beginTime) < 3000);
   // }

   // Test if deadlock detection works taking into account buffer pinning.
   @Test
   public void concurrencyTest4() {
      long beginTime = System.currentTimeMillis();

      Thread thread1 = new Thread(new Test4Thread1("thread1"), "thread1");
      Thread thread2 = new Thread(new Test4Thread2("thread2"), "thread2");
      thread1.start();
      thread2.start();

      try {
         thread1.join();
         thread2.join();
      } catch(InterruptedException e) {
         System.out.println(e.toString());
      }

      long endTime = System.currentTimeMillis();

      // assertTrue("Test 4: Deadlock should be detected in less than 3 seconds", (endTime - beginTime) < 3000);
   }
}

/*
 * Classes for Concurrency Test 2
 */

// This thread will start a transaction that pins block 2 first, then block 3.
class Test2Thread1 implements Runnable {
   String name;

   public Test2Thread1(String name) {
      this.name = name;
   }

   public void run() {
      Transaction transaction = new Transaction();
      Block block2 = new Block("student.tbl", 2);
      Block block3 = new Block("student.tbl", 3);

      try {
         // Update block 2, then wait so that the transaction in thread 2 has time to pin
         // block 3. We wait longer than that in that thread so that this one does not timeout.
         transaction.pin(block2);
         transaction.setInt(block2, 0, 2);
         Thread.sleep(2000);

         // Update block 3. The setInt call will not happen until
         // the transaction in thread 2 times out and rolls back.
         transaction.pin(block3);
         transaction.setInt(block3, 0, 3);

         // Unpin the blocks and commit the transaction.
         transaction.unpin(block2);
         transaction.unpin(block3);
         transaction.commit();
      } catch(InterruptedException e) {
         System.out.println(e.toString());
      } catch(LockAbortException e) {
         e.printStackTrace();
      }
   }
}

// This thread will start a transaction that pins block 3 first, then block 2.
class Test2Thread2 implements Runnable {
   String name;

   public Test2Thread2(String name) {
      this.name = name;
   }

   public void run() {
      Transaction transaction = new Transaction();
      Block block2 = new Block("student.tbl", 2);
      Block block3 = new Block("student.tbl", 3);

      try {
         // Update block 3, then wait so that the transaction
         // in thread 1 has time to pin block 2.
         transaction.pin(block3);
         transaction.setInt(block3, 0, 5);
         Thread.sleep(100);

         // Update block 2.
         transaction.pin(block2);
         transaction.setInt(block2, 0, 5);

         // Unpin the blocks and commit the transaction.
         transaction.unpin(block2);
         transaction.unpin(block3);
         transaction.commit();
      } catch(InterruptedException e) {
         System.out.println(e.toString());
      } catch(LockAbortException e) {
         transaction.rollback();
      }
   }
}

/*
 * Classes for Concurrency Test 3
 */

// The transaction in this thread obtains an xLock on block 6, and then tries to
// obtain an xLock on block 4, but does so after thread 2 obtains an sLock on it.
class Test3Thread1 implements Runnable {
   String name;

   public Test3Thread1(String name) {
      this.name = name;
   }

   public void run() {
      Transaction transaction = new Transaction();
      Block block6 = new Block("student.tbl", 6);
      Block block4 = new Block("student.tbl", 4);

      try {
         // Update block 6, then wait so that the transaction
         // in thread 2 has time to pin block 4.
         transaction.pin(block6);
         transaction.setInt(block6, 0, 6);
         Thread.sleep(100);

         // Update block 4.
         transaction.pin(block4);
         transaction.setInt(block4, 0, 1000);

         // Unpin the blocks and commit the transaction.
         transaction.unpin(block6);
         transaction.unpin(block4);
         transaction.commit();
      } catch(InterruptedException e) {
         System.out.println(e.toString());
      } catch(LockAbortException e) {
         transaction.rollback();
      }
   }
}

// The transaction in this thread obtains an sLock on block 4, and then tries to
// obtain an sLock on block 5 but does so after thread 3 obtains an xLock on it.
class Test3Thread2 implements Runnable {
   String name;

   public Test3Thread2(String name) {
      this.name = name;
   }

   public void run() {
      Transaction transaction = new Transaction();
      Block block4 = new Block("student.tbl", 4);
      Block block5 = new Block("student.tbl", 5);

      try {
         // Read from block 4, then wait so that the transaction
         // in thread 3 has time to pin block 5.
         transaction.pin(block4);
         transaction.getInt(block4, 0);
         Thread.sleep(100);

         // Read from block 5.
         transaction.pin(block5);
         transaction.getInt(block5, 0);

         // Unpin the blocks and commit the transaction.
         transaction.unpin(block4);
         transaction.unpin(block5);
         transaction.commit();
      } catch(InterruptedException e) {
         System.out.println(e.toString());
      } catch(LockAbortException e) {
         transaction.rollback();
      }
   }
}

// The transaction in this thread obtains an xLock on block 5, and then tries to
// obtain an sLock on block 6, but does so after thread 1 obtains an xLock on it.
class Test3Thread3 implements Runnable {
   String name;

   public Test3Thread3(String name) {
      this.name = name;
   }

   public void run() {
      Transaction transaction = new Transaction();
      Block block5 = new Block("student.tbl", 5);
      Block block6 = new Block("student.tbl", 6);

      try {
         // Write to block 5, then wait so that the transaction
         // in thread 1 has time to pin block 6.
         transaction.pin(block5);
         transaction.setInt(block5, 0, 5);
         Thread.sleep(100);

         // Read from block 6.
         transaction.pin(block6);
         transaction.getInt(block6, 0);

         // Unpin the blocks and commit the transaction.
         transaction.unpin(block5);
         transaction.unpin(block6);
         transaction.commit();
      } catch(InterruptedException e) {
         System.out.println(e.toString());
      } catch(LockAbortException e) {
         transaction.rollback();
      }
   }
}

/*
 * Classes for Concurrency Test 4
 */

class Test4Thread1 implements Runnable {
   String name;

   public Test4Thread1(String name) {
      this.name = name;
   }

   public void run() {
      Transaction transaction = new Transaction();
      Block block7 = new Block("student.tbl", 7);
      Block block8 = new Block("student.tbl", 8);
      Block block9 = new Block("student.tbl", 9);
      Block block10 = new Block("student.tbl", 10);
      Block block11 = new Block("student.tbl", 11);
      Block block12 = new Block("student.tbl", 12);

      Block block15 = new Block("student.tbl", 15);

      try {
         // Pin six blocks.
         transaction.pin(block7);
         transaction.pin(block8);
         transaction.pin(block9);
         transaction.pin(block10);
         transaction.pin(block11);
         transaction.pin(block12);

         transaction.setInt(block12, 0, 12);
         Thread.sleep(100);

         transaction.pin(block15);

         // Unpin the blocks and commit the transaction.
         transaction.unpin(block7);
         transaction.unpin(block8);
         transaction.unpin(block9);
         transaction.unpin(block10);
         transaction.unpin(block11);
         transaction.unpin(block12);
         transaction.unpin(block15);
         transaction.commit();
      } catch(InterruptedException e) {
         System.out.println(e.toString());
      } catch(LockAbortException e) {
         transaction.rollback();
      }
   }
}

class Test4Thread2 implements Runnable {
   String name;

   public Test4Thread2(String name) {
      this.name = name;
   }

   public void run() {
      Transaction transaction = new Transaction();
      Block block12 = new Block("student.tbl", 12);
      Block block13 = new Block("student.tbl", 13);
      Block block14 = new Block("student.tbl", 14);

      try {
         transaction.pin(block12);
         transaction.pin(block13);
         transaction.pin(block14);
         Thread.sleep(100);

         transaction.setInt(block12, 0, 12);

         // Unpin the blocks and commit the transaction.
         transaction.unpin(block12);
         transaction.unpin(block13);
         transaction.unpin(block14);
         transaction.commit();
      } catch(InterruptedException e) {
         System.out.println(e.toString());
      } catch(LockAbortException e) {
         transaction.rollback();
      }
   }
}
