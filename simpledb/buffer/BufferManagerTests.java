package simpledb.buffer;

import java.util.concurrent.TimeUnit;

import simpledb.record.*;
import static org.junit.Assert.*;
import org.junit.Test;

public class BufferManagerTests {

   // Tests the buffer manager pinNew method as well
   // as the get and set methods of the buffer class.
   @Test
   public void runTests() {
      BufferMgr bufferManager = new BufferMgr(5);
      Schema schema = new Schema();
      TableInfo tableInfo = new TableInfo("student", schema);
      RecordFormatter recordFormatter = new RecordFormatter(tableInfo);

      Buffer buffer1 = bufferManager.pinNew("student.tbl", recordFormatter);
      Buffer buffer2 = bufferManager.pinNew("student.tbl", recordFormatter);
      Buffer buffer3 = bufferManager.pinNew("student.tbl", recordFormatter);
      Buffer buffer4 = bufferManager.pinNew("student.tbl", recordFormatter);
      Buffer buffer5 = bufferManager.pinNew("student.tbl", recordFormatter);

      buffer1.setInt(0, 1, 0, 0);
      buffer1.setInt(5, 2, 0, 0);
      buffer2.setString(0, "Test", 0, 0);
      buffer3.setString(0, "Test1", 0, 0);
      buffer4.setInt(0, 4, 0, 0);
      buffer5.setInt(0, 5, 0, 0);

      // Test if the values were properly set in the buffers.
      assertEquals("First buffer should have value 1 at offset 0.", buffer1.getInt(0), 1);
      assertEquals("First buffer should have value 2 at offset 5.", buffer1.getInt(5), 2);
      assertEquals("Second buffer should have value Test at offset 0.", buffer2.getString(0), "Test");
      assertEquals("Third buffer should have value Test1 at offset 0.", buffer3.getString(0), "Test1");
      assertEquals("Fourth buffer should have value 3 at offset 0.", buffer4.getInt(0), 4);
      assertEquals("Fifth buffer should have value 4 at offset 0.", buffer5.getInt(0), 5);

      // Test if the buffers were pinned as expected.
      assertEquals("First buffer should have pin count of one.", buffer1.numPins(), 1);
      assertEquals("Second buffer should have pin count of one.", buffer2.numPins(), 1);
      assertEquals("Third buffer should have pin count of one.", buffer3.numPins(), 1);
      assertEquals("Fourth buffer should have pin count of one.", buffer4.numPins(), 1);
      assertEquals("Fifth buffer should have pin count of one.", buffer5.numPins(), 1);

      // Test the LRU buffer replacement strategy.

      // Notice we unpin in the order 4, 1, 5. So buffer
      // 4 is the least recently used, followed by 1.
      // We have the program sleep to ensure the buffers
      // have different last used times.
      try {
         bufferManager.unpin(buffer4);
         TimeUnit.MILLISECONDS.sleep(10);
         bufferManager.unpin(buffer1);
         TimeUnit.MILLISECONDS.sleep(10);
         bufferManager.unpin(buffer5);
      } catch(Exception e) {
         e.printStackTrace();
      }

      // This should replace buffer 4.
      Buffer replacement = bufferManager.pinNew("student.tbl", recordFormatter);
      assertEquals("Buffer 4 should have value 0 at offset 0", buffer4.getInt(0), 0);
      assertEquals("Buffer 1 should have value 1 at offset 0", buffer1.getInt(0), 1);
      assertEquals("Buffer 5 should have value 4 at offset 0", buffer5.getInt(0), 5);

      // Set a value for buffer 4 so that we will know if it is replaced.
      buffer4.setInt(0, 4, 0, 0);

      // This should replace buffer 1
      Buffer replacement1 = bufferManager.pinNew("student.tbl", recordFormatter);
      assertEquals("Buffer 4 should have value 4 at offset 0", buffer4.getInt(0), 4);
      assertEquals("Buffer 1 should have value 0 at offset 0", buffer1.getInt(0), 0);
      assertEquals("Buffer 5 should have value 5 at offset 0", buffer5.getInt(0), 5);

      // Set a value for buffer 1 so that we will know if it is replaced.
      buffer1.setInt(0, 1, 0, 0);

      Buffer replacement2 = bufferManager.pinNew("student.tbl", recordFormatter);
      assertEquals("Buffer 4 should have value 4 at offset 0", buffer4.getInt(0), 4);
      assertEquals("Buffer 1 should have value 1 at offset 0", buffer1.getInt(0), 1);
      assertEquals("Buffer 5 should have value 0 at offset 0", buffer5.getInt(0), 0);
   }
}
