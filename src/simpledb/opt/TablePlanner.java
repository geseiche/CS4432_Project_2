package simpledb.opt;

import simpledb.materialize.MergeJoinPlan;
import simpledb.tx.Transaction;
import simpledb.record.Schema;
import simpledb.query.*;
import simpledb.index.query.*;
import simpledb.metadata.IndexInfo;
import simpledb.multibuffer.MultiBufferProductPlan;
import simpledb.server.SimpleDB;
import java.util.Map;

/**
 * This class contains methods for planning a single table.
 * @author Edward Sciore
 */
class TablePlanner {
   private TablePlan myplan;
   private Predicate mypred;
   private Schema myschema;
   private Map<String,IndexInfo> indexes;
   private Transaction tx;
   
   /**
    * Creates a new table planner.
    * The specified predicate applies to the entire query.
    * The table planner is responsible for determining
    * which portion of the predicate is useful to the table,
    * and when indexes are useful.
    * @param tblname the name of the table
    * @param mypred the query predicate
    * @param tx the calling transaction
    */
   public TablePlanner(String tblname, Predicate mypred, Transaction tx) {
      this.mypred  = mypred;
      this.tx  = tx;
      myplan   = new TablePlan(tblname, tx);
      myschema = myplan.schema();
      indexes  = SimpleDB.mdMgr().getIndexInfo(tblname, tx);
   }
   
   /**
    * Constructs a select plan for the table.
    * The plan will use an indexselect, if possible.
    * @return a select plan for the table.
    */
   public Plan makeSelectPlan() {
      Plan p = makeIndexSelect();
      if (p == null)
         p = myplan;
      return addSelectPred(p);
   }
   
   /**
    * Constructs a join plan of the specified plan
    * and the table.  The plan will use an indexjoin, if possible.
    * (Which means that if an indexselect is also possible,
    * the indexjoin operator takes precedence.)
    * The method returns null if no join is possible.
    * @param current the specified plan
    * @return a join plan of the plan and this table
    */
   public Plan makeJoinPlan(Plan current) {
      Schema currsch = current.schema();
      Predicate joinpred = mypred.joinPred(myschema, currsch);
      if (joinpred == null)
         return null;
      Plan p = makeIndexJoin(current, currsch);
      if (p == null)
         p = makeProductJoin(current, currsch);
      return p;
   }

   /**
    * Constructs a join plan of the specified plan
    * and the table. The plan will use an MergeJoinPlan.
    * The method returns null if no join is possible.
    * @param current the specified plan
    * @return a join plan of the plan and this table
    */
   public Plan makeMergeJoinPlan(Plan current) {
      Schema currsch = current.schema();
      String tblname1 = myplan.tableName(); // Get the table name to pass down to SmartSortScan
      String tblname2 = current.tableName(); // Get the table name to pass down to SmartSortScan
      Plan p = makeMergeJoin(current, currsch, tblname1, tblname2);
      return p;
   }

   /**
    * Constructs a product plan of the specified plan and
    * this table.
    * @param current the specified plan
    * @return a product plan of the specified plan and this table
    */
   public Plan makeProductPlan(Plan current) {
      Plan p = addSelectPred(myplan);
      return new MultiBufferProductPlan(current, p, tx);
   }
   
   private Plan makeIndexSelect() {
      for (String fldname : indexes.keySet()) {
         Constant val = mypred.equatesWithConstant(fldname);
         if (val != null) {
            IndexInfo ii = indexes.get(fldname);
            return new IndexSelectPlan(myplan, ii, val, tx);
         }
      }
      return null;
   }
   
   private Plan makeIndexJoin(Plan current, Schema currsch) {
      for (String fldname : indexes.keySet()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            IndexInfo ii = indexes.get(fldname);
            Plan p = new IndexJoinPlan(current, myplan, ii, outerfield, tx);
            p = addSelectPred(p);
            return addJoinPred(p, currsch);
         }
      }
      return null;
   }
   
   private Plan makeProductJoin(Plan current, Schema currsch) {
      Plan p = makeProductPlan(current);
      return addJoinPred(p, currsch);
   }

   /**
    * CS4432: Creates the MergeJoinPlan.
    * @return A MergeJoinPlan to merge on, or null if no merge is possible (No like fields)
    */
   private Plan makeMergeJoin(Plan current, Schema currsch, String tblname1, String tblname2) {
      for (String fldname : myschema.fields()) {
         if (currsch.hasField(fldname)) {
            Plan p = new MergeJoinPlan(myplan, current, tblname1, tblname2, fldname, tx); // Create the MergeJoinPlan on the matching fields
            p = addSelectPred(p);
            return addJoinPred(p, currsch);
         }
      }
      return null;
   }
   
   private Plan addSelectPred(Plan p) {
      Predicate selectpred = mypred.selectPred(myschema);
      if (selectpred != null)
         return new SelectPlan(p, selectpred);
      else
         return p;
   }
   
   private Plan addJoinPred(Plan p, Schema currsch) {
      Predicate joinpred = mypred.joinPred(currsch, myschema);
      if (joinpred != null)
         return new SelectPlan(p, joinpred);
      else
         return p;
   }
}
