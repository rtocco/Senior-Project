package simpledb.parse;

import simpledb.query.*;
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

   /**
    * Save the table list and predicate, this constructor
    * is called if all fields are selected.
    */
   public QueryData(Collection<String> tables, Predicate pred) {
      allFields = true;
      this.fields = null;
      this.tables = tables;
      this.pred = pred;
   }

   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(Collection<String> fields, Collection<String> tables, Predicate pred) {
      allFields = false;
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
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
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
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
