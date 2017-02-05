package simpledb.buffer;

import simpledb.record.*;

public class BufferManagerTests {
    public BufferManagerTests() {}

    public static void runTests() {
        System.out.println("\nBuffer Manager Tests:");

        BufferMgr bufferManager = new BufferMgr(5);
        Schema schema = new Schema();
        TableInfo tableInfo = new TableInfo("cars", schema);
        RecordFormatter recordFormatter = new RecordFormatter(tableInfo);

        Buffer buffer = bufferManager.pinNew("cars.tbl", recordFormatter);
        System.out.println(buffer.isPinned());
        System.out.println(buffer.numPins());
    }
}
