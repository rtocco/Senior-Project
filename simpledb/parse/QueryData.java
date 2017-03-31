package simpledb.parse;

import simpledb.query.*;
import simpledb.materialize.AggregationFn;
import java.util.*;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private boolean allFields;
   private Collection<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   private Predicate groupPred;
   private Collection<String> groupByfields;
   private Collection<AggregationFn> aggregationFns;

   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(boolean allFields, Collection<String> fields, Collection<String> tables, Predicate pred, Predicate groupPred, Collection<String> groupByfields, Collection<AggregationFn> aggregationFns) {
      this.allFields = allFields;
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.groupPred = groupPred;
      this.groupByfields = groupByfields;
      this.aggregationFns = aggregationFns;

      if(groupByfields != null) {
         boolean containsAll = true;
         Iterator<String> iter = fields.iterator();
         while(iter.hasNext()) {
            String field = iter.next();
            if(!field.contains("count") && !field.contains("max") && !field.contains("min") && !field.contains("sum") && !field.contains("avg")) {
               if(!groupByfields.contains(field)) {
                  containsAll = false;
               }
            }
         }

         if(!containsAll) {
            throw new BadQueryException();
         }
      }
   }

   /**
    * returns true if all of the fields should be projected.
    */
   public boolean allFields() {
      return allFields;
   }

   /**
    * Returns the fields mentioned in the select clause.
    * @return a collection of field names
    */
   public Collection<String> fields() {
      return fields;
   }

   /**
    * Returns the tables mentioned in the from clause.
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }

   public Collection<AggregationFn> aggregationFns() {
      return aggregationFns;
   }

   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }

   /**
    * Returns the predicate for after a group by clause.
    */
   public Predicate groupPred() {
      return groupPred;
   }

   /**
    * Returns the fields mentioned in the group by clause.
    */
   public Collection<String> groupByfields() {
      return groupByfields;
   }

   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      return result;
   }
}
