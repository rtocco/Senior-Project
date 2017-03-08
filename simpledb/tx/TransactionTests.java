package simpledb.tx;

import static org.junit.Assert.*;
import org.junit.Test;
import java.util.Iterator;

import simpledb.file.Block;
import simpledb.record.*;
import simpledb.tx.recovery.*;
import simpledb.tx.concurrency.*;
import simpledb.buffer.BufferAbortException;

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
   @Test
   public void concurrencyTest1() {
      System.out.println("\nConcurrency Test 1");

      Transaction transaction = new Transaction();
      Block block = new Block("student.tbl", 1);
      transaction.pin(block);

      assertEquals("Value should initially be 0", transaction.getInt(block, 0), 0);
      transaction.setInt(block, 0, 5);
      assertEquals("Value should now be 5", transaction.getInt(block, 0), 5);

      transaction.setInt(block, 0, 10);
      assertEquals("Value should now be 10", transaction.getInt(block, 0), 10);
   }

   // Create deadlock using two processes, both writing to blocks.
   @Test
   public void concurrencyTest2() {
      System.out.println("\nConcurrency Test 2");

      // Keep track of the time.
      long beginTime = System.currentTimeMillis();

      // Start two transactions that will cause deadlock to occur.
      Thread thread1 = new Thread(new Test2Thread1("thread1"), "thread1");
      Thread thread2 = new Thread(new Test2Thread2("thread2"), "thread2");
      thread1.start();
      thread2.start();

      try {
         thread1.join();
         thread2.join();
      } catch(InterruptedException e) {
         System.out.println(e.toString());
      }

      long endTime = System.currentTimeMillis();

      // Initial deadlock detection timed out after 5 seconds. New one should work quickly.
      assertTrue("Test 2: Deadlock should be detected in less than 1 second", (endTime - beginTime) < 1000);

      // Test if thread 1 changed the values in blocks 1 and 2.
      Transaction transaction = new Transaction();
      Block block2 = new Block("student.tbl", 2);
      Block block3 = new Block("student.tbl", 3);
      transaction.pin(block2);
      transaction.pin(block3);

      assertEquals("Block 2 should equal 2 at offset 0", transaction.getInt(block2, 0), 2);
      assertEquals("Block 3 should equal 3 at offset 0", transaction.getInt(block3, 0), 3);
   }

   // Test deadlock with 3 transactions and a mixture of sLocks and xLocks
   @Test
   public void concurrencyTest3() {
      System.out.println("\nConcurrency Test 3");

      long beginTime = System.currentTimeMillis();

      // Start three transactions that will cause deadlock to occur.
      Thread thread1 = new Thread(new Test3Thread1("thread1"), "thread1");
      Thread thread2 = new Thread(new Test3Thread2("thread2"), "thread2");
      Thread thread3 = new Thread(new Test3Thread3("thread3"), "thread3");
      thread1.start();
      thread2.start();
      thread3.start();

      try {
         thread1.join();
         thread2.join();
         thread3.join();
      } catch(InterruptedException e) {
         System.out.println(e.toString());
      }

      long endTime = System.currentTimeMillis();

      // Deadlock should be detected immediately.
      assertTrue("Test 3: Deadlock should be detected in less than 1 second", (endTime - beginTime) < 1000);
   }

   // Test if deadlock detection works taking into account buffer pinning.
   @Test
   public void concurrencyTest4() {
      System.out.println("\nConcurrency Test 4");

      long beginTime = System.currentTimeMillis();

      // Start two transactions that will cause deadlock to occur partially because of buffer conflicts
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

      // Deadlock should be detected immediately.
      assertTrue("Test 4: Deadlock should be detected in less than 1 second", (endTime - beginTime) < 1000);
   }

   // Test to make sure the new deadlock detection does not cause false negatives.
   @Test
   public void concurrencyTest5() {
      System.out.println("\nConcurrency Test 5");

      // Start two transactions, thread2 is dependent on thread1.
      Thread thread1 = new Thread(new Test5Thread1("thread1"), "thread1");
      Thread thread2 = new Thread(new Test5Thread2("thread2"), "thread2");
      thread1.start();
      thread2.start();

      try {
         thread1.join();
         thread2.join();
      } catch(InterruptedException e) {
         System.out.println(e.toString());
      }

      Transaction transaction = new Transaction();
      Block block16 = new Block("student.tbl", 16);
      Block block17 = new Block("student.tbl", 17);
      transaction.pin(block16);
      transaction.pin(block17);

      assertEquals("Block 16 should equal 16 at offset 0", transaction.getInt(block16, 0), 16);
      assertEquals("Block 17 should equal 17 at offset 0", transaction.getInt(block17, 0), 17);
   }

   // Test to ensure false negatives do not occur when the buffer manager runs out of buffers.
   @Test
   public void concurrencyTest6() {
      System.out.println("\nConcurrency Test 6");

      // Start two transactions, thread2 is dependent on thread1.
      Thread thread1 = new Thread(new Test6Thread1("thread1"), "thread1");
      Thread thread2 = new Thread(new Test6Thread2("thread1"), "thread2");
      thread1.start();
      thread2.start();

      try {
         thread1.join();
         thread2.join();
      } catch(InterruptedException e) {
         System.out.println(e.toString());
      }

      Transaction transaction = new Transaction();
      Block block25 = new Block("student.tbl", 25);
      Block block26 = new Block("student.tbl", 26);
      transaction.pin(block25);
      transaction.pin(block26);

      assertEquals("Block 25 should equal 25 at offset 0", transaction.getInt(block25, 0), 25);
      assertEquals("Block 26 should equal 26 at offset 0", transaction.getInt(block26, 0), 26);
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
         transaction.setInt(block2, 0, 5);
         Thread.sleep(100);

         // Update block 3. The setInt call will not happen until
         // the transaction in thread 2 times out and rolls back.
         transaction.pin(block3);
         transaction.setInt(block3, 0, 5);

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
         // Update block 3
         transaction.pin(block3);
         transaction.setInt(block3, 0, 3);

         // Update block 2.
         transaction.pin(block2);
         transaction.setInt(block2, 0, 2);

         // Unpin the blocks and commit the transaction.
         transaction.unpin(block2);
         transaction.unpin(block3);
         transaction.commit();
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
         System.out.println("Thread 1 rollback");
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
         System.out.println("Thread 2 rollback");
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
         System.out.println("Thread 3 rollback");
         transaction.rollback();
      }
   }
}

/*
 * Classes for Concurrency Test 4
 */

// Pins six blocks, obtains an xLock on block 12, and then waits.
// Then tries to pin block 15, but cannot because there thread 2
// will have the remaining buffers in the buffer pool.
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

         // Obtain an xLock on block 12 and wait.
         transaction.setInt(block12, 0, 12);
         Thread.sleep(200);

         // Try to pin block 15. Deadlock should be detected.
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
      } catch(BufferAbortException e) {
         transaction.rollback();
      }
   }
}

// This thread pins three blocks, waits, and then tries to obtain an xLock on block 12.
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
         // Pin three blocks.
         Thread.sleep(50);
         transaction.pin(block12);
         transaction.pin(block13);
         transaction.pin(block14);
         Thread.sleep(100);

         // Attempt to obtain an xLock on block 12, will have to wait for thread 1.
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
      } catch(BufferAbortException e) {
         transaction.rollback();
      }
   }
}

/*
 * Classes for Concurrency Test 5
 */

// Obtains and xLock on block 16 and an sLock on block 17.
class Test5Thread1 implements Runnable {
   String name;

   public Test5Thread1(String name) {
      this.name = name;
   }

   public void run() {
      Transaction transaction = new Transaction();
      Block block16 = new Block("student.tbl", 16);
      Block block17 = new Block("student.tbl", 17);

      try {
         transaction.pin(block16);
         transaction.pin(block17);

         // Obtain an xLock on block 16 and an sLock on block 17, then wait.
         transaction.setInt(block16, 0, 16);
         transaction.getInt(block17, 0);
         Thread.sleep(500);

         // Unpin the blocks and commit the transaction.
         transaction.unpin(block16);
         transaction.unpin(block17);
         transaction.commit();
      } catch(InterruptedException e) {
         System.out.println(e.toString());
      } catch(LockAbortException e) {
         transaction.rollback();
      } catch(BufferAbortException e) {
         transaction.rollback();
      }
   }
}

// Obtains an sLock on block 16 and an sLock on block 17.
class Test5Thread2 implements Runnable {
   String name;

   public Test5Thread2(String name) {
      this.name = name;
   }

   public void run() {
      Transaction transaction = new Transaction();
      Block block16 = new Block("student.tbl", 16);
      Block block17 = new Block("student.tbl", 17);

      try {
         Thread.sleep(100);
         transaction.pin(block16);
         transaction.pin(block17);

         // Will have to wait for thread 1 to finish with the blocks.
         transaction.getInt(block16, 0);
         transaction.setInt(block17, 0, 17);

         // Unpin the blocks and commit the transaction.
         transaction.unpin(block16);
         transaction.unpin(block17);
         transaction.commit();
      } catch(InterruptedException e) {
         System.out.println(e.toString());
      } catch(LockAbortException e) {
         transaction.rollback();
      } catch(BufferAbortException e) {
         transaction.rollback();
      }
   }
}

/*
 * Classes for concurrency test 6.
 */

// Pins 7 blocks and waits.
class Test6Thread1 implements Runnable {
   String name;

   public Test6Thread1(String name) {
      this.name = name;
   }

   public void run() {
      Transaction transaction = new Transaction();
      Block block18 = new Block("student.tbl", 18);
      Block block19 = new Block("student.tbl", 19);
      Block block20 = new Block("student.tbl", 20);
      Block block21 = new Block("student.tbl", 21);
      Block block22 = new Block("student.tbl", 22);
      Block block23 = new Block("student.tbl", 23);
      Block block24 = new Block("student.tbl", 24);

      try {
         // Pin 7 blocks.
         transaction.pin(block18);
         transaction.pin(block19);
         transaction.pin(block20);
         transaction.pin(block21);
         transaction.pin(block22);
         transaction.pin(block23);
         transaction.pin(block24);

         Thread.sleep(200);

         // Unpin the blocks and commit the transaction.
         transaction.unpin(block18);
         transaction.unpin(block19);
         transaction.unpin(block20);
         transaction.unpin(block21);
         transaction.unpin(block22);
         transaction.unpin(block23);
         transaction.unpin(block24);
         transaction.commit();
      } catch(InterruptedException e) {
         System.out.println(e.toString());
      } catch(LockAbortException e) {
         transaction.rollback();
      } catch(BufferAbortException e) {
         transaction.rollback();
      }
   }
}

// Pins two blocks unrelated to those thread 1 is using.
class Test6Thread2 implements Runnable {
   String name;

   public Test6Thread2(String name) {
      this.name = name;
   }

   public void run() {
      Transaction transaction = new Transaction();
      Block block25 = new Block("student.tbl", 25);
      Block block26 = new Block("student.tbl", 26);

      try {
         Thread.sleep(100);

         // Pins two blocks, will have to wait for thread 1 to pin block 26.
         transaction.pin(block25);
         transaction.pin(block26);

         // Write to blocks 25 and 26 so that it can be tested that it successfully pinned the blocks.
         transaction.setInt(block25, 0, 25);
         transaction.setInt(block26, 0, 26);

         // Unpin the blocks and commit the transaction.
         transaction.unpin(block25);
         transaction.unpin(block26);
         transaction.commit();
      } catch(InterruptedException e) {
         System.out.println(e.toString());
      } catch(LockAbortException e) {
         transaction.rollback();
      } catch(BufferAbortException e) {
         transaction.rollback();
      }
   }
}
