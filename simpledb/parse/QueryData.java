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
   private String joinType;
   private Predicate pred;
   private Predicate groupPred;
   private Collection<String> groupByfields;
   private Collection<AggregationFn> aggregationFns;
   private String orderBy;

   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(boolean allFields, Collection<String> fields, Collection<String> tables, String joinType, Predicate pred, Predicate groupPred, Collection<String> groupByfields, Collection<AggregationFn> aggregationFns, String orderBy) {
      this.allFields = allFields;
      this.fields = fields;
      this.tables = tables;
      this.joinType = joinType;
      this.pred = pred;
      this.groupPred = groupPred;
      this.groupByfields = groupByfields;
      this.aggregationFns = aggregationFns;
      this.orderBy = orderBy;

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

   /**
    * Returns the type of join for this query,
    * or a blank string if there is no join.
    */
   public String joinType() {
      return joinType;
   }

   public Collection<AggregationFn> aggregationFns() {
      return aggregationFns;
   }

   public String orderBy() {
      return orderBy;
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
