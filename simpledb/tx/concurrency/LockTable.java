package simpledb.tx.concurrency;

import simpledb.file.Block;
import simpledb.server.SimpleDB;

import java.util.*;

/**
 * The lock table, which provides methods to lock and unlock blocks.
 * If a transaction requests a lock that causes a conflict with an
 * existing lock, then that transaction is placed on a wait list.
 * There is only one wait list for all blocks.
 * When the last lock on a block is unlocked, then all transactions
 * are removed from the wait list and rescheduled.
 * If one of those transactions discovers that the lock it is waiting for
 * is still locked, it will place itself back on the wait list.
 * @author Edward Sciore
 */
public class LockTable {
   private static final long MAX_TIME = 5000; // 10 seconds

   // Key: block, Value: transactions #'s with xLocks on the Block
   private Map<Block,Integer> xLocks = new HashMap<Block,Integer>();
   // Key: block, Value: ArrayList of transactions #'s with sLocks on the block
   private Map<Block, ArrayList<Integer>> sLocks = new HashMap<Block, ArrayList<Integer>>();
   // Keeps track of transactions stalled waiting for a buffer; Key: transaction #, Value: true/false
   private HashMap<Integer, Boolean> bufferWaitList = new HashMap<Integer, Boolean>();
   // Graph that keeps track of transactions waiting for locks
   private HashMap<Integer, ArrayList<Node>> waitsFor = new HashMap<Integer, ArrayList<Node>>();

   public LockTable() {}

   // Keep track of the transactions waiting for a buffer.
   public void putOnWaitList(int txnum) {
      bufferWaitList.put(txnum, true);
   }

   // Keep track of the transactions waiting for a buffer.
   public void takeOffWaitList(int txnum) {
      bufferWaitList.put(txnum, false);
   }

   /**
    * Returns true if having this transaction
    * wait for a buffer will not cause deadlock.
    */
   public synchronized boolean noBufferConflicts(int txnum) {
      Set<Integer> set = bufferWaitList.keySet();
      Iterator<Integer> keys = set.iterator();

      // We must check to see if every transaction waiting for a buffer is also waiting for this transaction.
      boolean allHaveConflicts = true;

      // Loop through every transaction in the wait list.
      while(keys.hasNext()) {
         // Transaction # of the waiting transaction.
         int key = keys.next();
         // We don't need to look at the transaction waiting for the buffer.
         if(key != txnum && bufferWaitList.get(key)) {
            // Look for a cycle in the waitsFor graph.
            HashMap<Integer, Boolean> visitList = new HashMap<Integer, Boolean>();
            boolean cycleFound = depthFirstSearch(key, txnum, visitList, true);

            // If a cycle was not found for a transaction, it will
            // eventually release the buffer, so deadlock will not occur.
            if(cycleFound == false) {
               allHaveConflicts = false;
               break;
            }
         }
      }

      return !allHaveConflicts;
   }

   /**
    * Returns true if having this transaction
    * wait for a lock will not cause deadlock.
    */
   public synchronized boolean noLockConflicts(int txnum) {
      HashMap<Integer, Boolean> visitList = new HashMap<Integer, Boolean>();
      boolean cycleFound = depthFirstSearch(txnum, txnum, visitList, false);

      return !cycleFound;
   }

   /**
    * Perform a DFS through the waitsFor graph, looking for a cycle.
    * @param   current: The current txnum being investigated.
    * @param   txnum: The txnum of the transaction that we wish to know whether it will cause deadlock.
    * @param   visitList: A hashmap of nodes in the graph that the DFS has already visited.
    * @param   calledForBuffer: True if this is being called from noBufferConflicts, false if from noLockConflicts
    * @return  True if a cycle was found, false otherwise.
    */
   private boolean depthFirstSearch(int current, int txnum, HashMap<Integer, Boolean> visitList, boolean calledForBuffer) {
      // Get the list of transactions current is waiting for.
      ArrayList<Node> row = waitsFor.get(current);

      // If current isn't waiting for any locks, we see if it's waiting for a buffer.
      if(row == null) {
         if(bufferWaitList.get(current) == null) {
            return false;
         }
         if(calledForBuffer == true) {
            return false;
         }
         if(!noBufferConflicts(current)) {
            return true;
         }
         return false;
      }

      // Search "depth first" by calling this method recursively on each transaction current is waiting for.
      for(int i = 0; i < row.size(); i++) {
         if(row.get(i).txnum == txnum) {
            return true;
         }

         // If we haven't visited the next node yet, visit it looking for a cycle.
         boolean cycleFound = false;
         if(visitList.get(row.get(i).txnum) == null) {
            visitList.put(row.get(i).txnum, true);
            cycleFound = depthFirstSearch(row.get(i).txnum, txnum, visitList, calledForBuffer);
         }

         if(cycleFound == true) {
            return true;
         }
      }

      return false;
   }

   /**
    * Grants an SLock on the specified block.
    * If an XLock exists when the method is called,
    * then the calling thread will be placed on a wait list
    * until the lock is released.
    * If the thread remains on the wait list for a certain
    * amount of time (currently 10 seconds),
    * then an exception is thrown.
    * @param blk a reference to the disk block
    */
   public synchronized void sLock(Block blk, int txnum) {
      try {
         long timestamp = System.currentTimeMillis();
         int possessor;

         // Transaction txnum is waiting for transaction possessor.
         if((possessor = hasXlock(blk)) != -1) {
            ArrayList<Integer> possessors = new ArrayList<Integer>();
            possessors.add(possessor);
            addToGraph(txnum, possessors, blk);

            // If a cycle exists in the waitsFor graph because of this transaction.
            if(noLockConflicts(txnum) == false) {
               throw new LockAbortException();
            }
         }

         // Wait for the lock to become available.
         while ((possessor != -1) && !waitingTooLong(timestamp)) {
            wait(MAX_TIME);
            possessor = hasXlock(blk);
         }
         // Transaction is no longer waiting.
         removeFromGraph(txnum);

         if (hasXlock(blk) != -1) {
            throw new LockAbortException();
         }

         if(sLocks.get(blk) == null) {
            sLocks.put(blk, new ArrayList<Integer>());
         }
         // If another transaction is waiting for an xLock on this
         // block, add this transaction to the ones it is waiting for.
         addSLockToGraph(txnum, blk);

         sLocks.get(blk).add(txnum);
      }
      catch(InterruptedException e) {
         throw new LockAbortException();
      }
   }

   /**
    * Grants an XLock on the specified block.
    * If a lock of any type exists when the method is called,
    * then the calling thread will be placed on a wait list
    * until the locks are released.
    * If the thread remains on the wait list for a certain
    * amount of time (currently 10 seconds),
    * then an exception is thrown.
    * @param blk a reference to the disk block
    */
   synchronized void xLock(Block blk, int txnum) {
      try {
         long timestamp = System.currentTimeMillis();
         ArrayList<Integer> possessors;

         // Transaction txnum is waiting for transactions possessors.
         if((possessors = hasOtherSLocks(blk)).size() > 1) {
            addToGraph(txnum, possessors, blk);

            // If a cycle exists in the waitsFor graph because of this transaction.
            if(noLockConflicts(txnum) == false) {
               throw new LockAbortException();
            }
         }

         // Wait for a lock to become available.
         while ((possessors.size() > 1) && !waitingTooLong(timestamp)) {
            wait(MAX_TIME);
            possessors = hasOtherSLocks(blk);
         }
         // Transaction is no longer waiting.
         removeFromGraph(txnum);

         if (hasOtherSLocks(blk).size() > 1) {
            throw new LockAbortException();
         }

         xLocks.put(blk, txnum);
      }
      catch(InterruptedException e) {
         throw new LockAbortException();
      }
   }

   /**
    * Releases a lock on the specified block.
    * If this lock is the last lock on that block,
    * then the waiting transactions are notified.
    * @param blk a reference to the disk block
    */
   synchronized void unlock(Block blk, int txnum) {
      removeFromGraph(txnum, blk);

      if(xLocks.get(blk) != null) {
         xLocks.remove(blk);
      }

      if(sLocks.get(blk).size() > 0) {
         sLocks.get(blk).remove(sLocks.get(blk).indexOf(txnum));
      }
      notifyAll();
   }

   /**
    * Adds to the waitsFor graph a transaction waiting for other transactions.
    * @param  waiter: the txnum of the waiting transaction
    * @param  possessors: the txnums of the transactions it's waiting for
    * @param  blk: the block in dispute
    */
   private synchronized void addToGraph(int waiter, ArrayList<Integer> possessors, Block blk) {
      ArrayList<Node> row = waitsFor.get(waiter);

      if(row == null) {
         row = new ArrayList<Node>();
      }

      for(int i = 0; i < possessors.size(); i++) {
         if(possessors.get(i) != waiter) {
            row.add(new Node(possessors.get(i), blk.number()));
         }
      }

      waitsFor.put(waiter, row);
   }

   /**
    * If another transaction is waiting for an xLock on this
    * block, add this transaction to the ones it is waiting for.
    * @param  txnum: the transaction to be added
    * @param  blk: the blk in dispute
    */
   private synchronized void addSLockToGraph(int txnum, Block blk) {
      Set<Integer> set = waitsFor.keySet();
      Iterator<Integer> keys = set.iterator();

      // Loop through each of the waiting transactions.
      while(keys.hasNext()) {
         int key = keys.next();
         ArrayList<Node> row = waitsFor.get(key);

         // If it is waiting for another transaction.
         if(row.size() > 0) {
            // If it is waiting for a transaction with this block number.
            if(row.get(0).blkNum == blk.number()) {
               row.add(new Node(txnum, blk.number()));
               waitsFor.put(key, row);
            }
         }
      }
   }

   // Remove the entry in the waitsFor graph where this transaction is waiting on others.
   private void removeFromGraph(int txnum) {
      waitsFor.remove(txnum);
   }

   // Remove all of the nodes in the waitsFor graph where this transaction possesses this block.
   private void removeFromGraph(int txnum, Block blk) {
      Set<Integer> set = waitsFor.keySet();
      Iterator<Integer> keys = set.iterator();

      while(keys.hasNext()) {
         int key = keys.next();
         ArrayList<Node> row = waitsFor.get(key);

         for(int i = 0; i < row.size(); i++) {
            Node node = row.get(i);
            if(node.txnum == txnum && node.blkNum == blk.number()) {
               row.remove(i);
               waitsFor.put(key, row);
               break;
            }
         }
      }
   }

   private int hasXlock(Block blk) {
      if(xLocks.get(blk) == null) {
         return -1;
      }
      return xLocks.get(blk);
   }

   private ArrayList<Integer> hasOtherSLocks(Block blk) {
      return sLocks.get(blk);
   }

   private boolean waitingTooLong(long starttime) {
      return System.currentTimeMillis() - starttime > MAX_TIME;
   }

   private int indexOfTx(ArrayList<Node> row, int txnum) {
      for(int i = 0; i < row.size(); i++) {
         if(row.get(i).txnum == txnum) {
            return i;
         }
      }

      return -1;
   }

   private class Node {
      public int txnum;
      public int blkNum;

      public Node(int txnum, int blkNum) {
         this.txnum = txnum;
         this.blkNum = blkNum;
      }
   }
}
