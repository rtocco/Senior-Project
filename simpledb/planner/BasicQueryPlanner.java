package simpledb.planner;

import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.parse.*;
import simpledb.materialize.*;
import simpledb.server.SimpleDB;
import java.util.*;

import simpledb.record.*;

/**
 * The simplest, most naive query planner possible.
 * @author Edward Sciore
 */
public class BasicQueryPlanner implements QueryPlanner {

   /**
    * Creates a query plan as follows.  It first takes
    * the product of all tables and views; it then selects on the predicate;
    * and finally it projects on the field list.
    */
   public Plan createPlan(QueryData data, Transaction tx) {
      //Step 1: Create a plan for each mentioned table or view
      List<Plan> plans = new ArrayList<Plan>();
      for (String tblname : data.tables()) {
         String viewdef = SimpleDB.mdMgr().getViewDef(tblname, tx);
         if (viewdef != null)
            plans.add(SimpleDB.planner().createQueryPlan(viewdef, tx));
         else
            plans.add(new TablePlan(tblname, tx));
      }

      // Create the product of all table plans
      Plan p = plans.remove(0);
      for (Plan nextplan : plans)
         p = new ProductPlan(p, nextplan);

      // Add a selection plan for the predicate
      if(data.joinType().equals("") || data.joinType().equals("inner")) {
         p = new SelectPlan(p, data.pred());
      }

      // Group together by certain fields. Materialize first for more efficient
      if(data.groupByfields() != null) {
         p = new MaterializePlan(p, tx);
         p = new GroupByPlan(p, data.groupByfields(), data.aggregationFns(), tx);
         if(data.groupPred() != null) {
            p = new SelectPlan(p, data.groupPred());
         }
      }

      // Project on the field names
      if(!data.allFields()) {
         p = new ProjectPlan(p, data.fields());
      }
      return p;
   }
}
