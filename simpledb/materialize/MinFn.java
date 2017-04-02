package simpledb.materialize;

import simpledb.query.*;

/**
 * The <i>min</i> aggregation function.
 */
public class MinFn implements AggregationFn {
   private String fldname;
   private Constant val;

   /**
    * Creates a min aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public MinFn(String fldname) {
      this.fldname = fldname;
   }

   /**
    * Starts a new minimum to be the
    * field value in the current record.
    * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
    */
   public void processFirst(Scan s) {
      val = s.getVal(fldname);
   }

   /**
    * Replaces the current minimum by the field value
    * in the current record, if it is lower.
    * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
    */
   public void processNext(Scan s) {
      Constant newval = s.getVal(fldname);
      if (newval.compareTo(val) < 0)
         val = newval;
   }

   /**
    * Returns the field's name, prepended by "minof".
    * @see simpledb.materialize.AggregationFn#fieldName()
    */
   public String fieldName() {
      return "minof" + fldname;
   }

   /**
    * Returns the current minimum.
    * @see simpledb.materialize.AggregationFn#value()
    */
   public Constant value() {
      return val;
   }
}