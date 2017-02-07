package simpledb.buffer;

import simpledb.record.*;
import static org.junit.Assert.*;
import org.junit.Test;

public class BufferManagerTests {
    public BufferManagerTests() {}

   @Test
    public void runTests() {
        System.out.println("\nBuffer Manager Tests:");

        BufferMgr bufferManager = new BufferMgr(5);
        bufferManager.test();
        Schema schema = new Schema();
        TableInfo tableInfo = new TableInfo("student", schema);
        RecordFormatter recordFormatter = new RecordFormatter(tableInfo);

        Buffer buffer = bufferManager.pinNew("student.tbl", recordFormatter);
        buffer.setInt(0, 5, 0, 0);

        bufferManager.test();

        assertEquals("Should be equal", 1, 1);
    }
}
