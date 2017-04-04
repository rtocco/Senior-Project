package simpledb.materialize;

import simpledb.query.*;

/**
 * The Scan class for the <i>leftjoin</i> operator.
 * @author Edward Sciore
 */
public class LeftJoinScan implements Scan {
   private Scan s1;
   private SortScan s2;
   private String fldname1, fldname2;
   private Constant joinval = null;
   private boolean s2Null = true;
   private boolean s1Done = false;
   private boolean s2Done = false;
   private boolean begun = false;

   /**
    * Creates a leftjoin scan for the two underlying sorted scans.
    * @param s1 the LHS sorted scan
    * @param s2 the RHS sorted scan
    * @param fldname1 the LHS join field
    * @param fldname2 the RHS join field
    */
   public LeftJoinScan(Scan s1, SortScan s2, String fldname1, String fldname2) {
      this.s1 = s1;
      this.s2 = s2;
      this.fldname1 = fldname1;
      this.fldname2 = fldname2;
      beforeFirst();
   }

   /**
    * Positions the scan before the first record,
    * by positioning each underlying scan before
    * their first records.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      s1.beforeFirst();
      s2.beforeFirst();
   }

   /**
    * Closes the scan by closing the two underlying scans.
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      s1.close();
      s2.close();
   }

   /**
    * Moves to the next record.  This is where the action is.
    * <P>
    * If it is the first record or the right scan has no more records, calls
    * an accessory function for dealing with that. If the left scan has no more
    * records, return false. Otherwise, compares the scan values from the
    * previous calls to next(). If the left side value is less than the right,
    * it moves the left one forward, and then moves the right side forward until
    * it is greater than or equal to the left side. Otherwise, it moves the
    * right side forward and checks whether or not it is still the same value.
    */
   public boolean next() {
      s2Null = false;
      if(begun == false) return firstNext();
      if(s1Done == true) return false;
      if(s2Done == true) return noS2Next();

      Constant v1 = s1.getVal(fldname1);
      Constant v2 = s2.getVal(fldname2);

      // If the left side value is less than the right side value.
      if(v1.compareTo(v2) < 0) {
         if(s1.next()) {
            // Move the right side forward until it is greater than or equal to the left.
            findRightScanValue();
            return true;
         }
         s1Done = true;
         return false;

      // If the right side value is the same as the left side value.
      } else {
         // Move the right side forward and check if it's still the same value.
         if(s2.next()) {
            Constant nextV2 = s2.getVal(fldname2);
            if(nextV2.compareTo(v1) > 0) {
               // The next right hand side value is different.
               // We move the left scan to the next record.
               if(s1.next()) {
                  Constant nextV1 = s1.getVal(fldname1);
                  // If the next left value is the same, move the right
                  // scan back to the first position having that value.
                  if(v1.compareTo(nextV1) == 0) {
                     s2.restorePosition();
                     return true;
                  }
                  // Move the right scan forward until it is greater than or equal to the left.
                  findRightScanValue();
                  return true;
               }
               s1Done = true;
               return false;
            } else {
               return true;
            }
         }
         // There are no more records in the right scan. If the next left side
         // record has the same value we must move the right scan back to the
         // first record having that value.
         if(s1.next()) {
            Constant nextV1 = s1.getVal(fldname1);
            if(v1.compareTo(nextV1) == 0) {
               s2.restorePosition();
               return true;
            }
            s2Done = true;
            return true;
         }
         s1Done = true;
         return false;
      }
   }

   /**
    * This is the procedure for the first time next is called.
    */
   private boolean firstNext() {
      begun = true;
      if(s1.next()) {
         if(s2.next()) {
            Constant v1 = s1.getVal(fldname1);
            Constant v2 = s2.getVal(fldname2);
            if(v1.compareTo(v2) < 0) {
               s2Null = true;
               return true;
            } else if(v1.compareTo(v2) > 0) {
               // Move the right scan forward until it is greater than or equal to the left.
               findRightScanValue();
               return true;
            } else {
               s2.savePosition();
               return true;
            }
         }
         s2Done = true;
         return true;
      }
      s1Done = true;
      return false;
   }

   /**
    * There are no more records in the right
    * scan. Only get values from the left.
    */
   private boolean noS2Next() {
      if(s1.next()) {
         return true;
      }
      s1Done = true;
      return false;
   }

   /**
    * Loop through the right side scan until the value
    * is greater than or equal to the left value.
    */
   private void findRightScanValue() {
      boolean greater = false;
      while(!greater) {
         Constant v1 = s1.getVal(fldname1);
         Constant v2 = s2.getVal(fldname2);
         if(v1.compareTo(v2) < 0) {
            s2Null = true;
            greater = true;
         } else if(v1.compareTo(v2) > 0) {
            if(!s2.next()) {
               s2Done = true;
               greater = true;
            }
         } else {
            s2.savePosition();
            greater = true;
         }
      }
   }

   /**
    * Returns the value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      if (s1.hasField(fldname)) {
         if(s1Done) {
            return null;
         } else {
            return s1.getVal(fldname);
         }
      } else {
         if(s2Null || s2Done) {
            return null;
         } else {
            return s2.getVal(fldname);
         }
      }
   }

   /**
    * Returns the integer value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
   public int getInt(String fldname) {
      if (s1.hasField(fldname))
         return s1.getInt(fldname);
      else
         return s2.getInt(fldname);
   }

   /**
    * Returns the string value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
   public String getString(String fldname) {
      if (s1.hasField(fldname))
         return s1.getString(fldname);
      else
         return s2.getString(fldname);
   }

   /**
    * Returns true if the specified field is in
    * either of the underlying scans.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return s1.hasField(fldname) || s2.hasField(fldname);
   }
}
