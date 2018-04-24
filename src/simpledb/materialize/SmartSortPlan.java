package simpledb.materialize;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;

import java.util.*;

/**
 * The Plan class for the <i>sort</i> operator.
 * @author Edward Sciore
 */
public class SmartSortPlan implements Plan {
   private Plan p;
   private Transaction tx;
   private Schema sch;
   private RecordComparator comp;
   private String tblname;
   private List<String> sortFields;
   
   /**
    * Creates a sort plan for the specified query.
    * @param p the plan for the underlying query
    * @param sortfields the fields to sort by
    * @param tx the calling transaction
    */
   public SmartSortPlan(Plan p, String tblname, List<String> sortfields, Transaction tx) { //TODO: Edit to take table names and setup the SmartSortScan properly
      this.p = p;
      this.tblname = tblname;
      this.sortFields = sortfields;
      this.tx = tx;
      sch = p.schema();
      comp = new RecordComparator(sortfields);
   }
   
   /**
    * This method is where most of the action is.
    * Up to 2 sorted temporary tables are created,
    * and are passed into SmartSortScan for final merging.
    * @see simpledb.query.Plan#open()
    */
   public Scan open() {
      TableInfo tblInfo = SimpleDB.mdMgr().getTableInfo(tblname, tx);
      List<TempTable> runs = null;
      if(!tblInfo.sorted.isEmpty() && sortFields.contains(tblInfo.sorted)) {
         Scan src = p.open();
         runs = getSortedTable(src);
         src.close();
      }
      else {
         Scan src = p.open();
         List<TempTable> tempList = splitIntoRuns(src);
         while (tempList.size() > 2) {
            tempList = doAMergeIteration(tempList);
         }
         src.close();
         performSort(new SmartSortScan(tempList, tblname, comp), tblInfo, tx);

         //After sort, reopen the record file
         src = p.open();
         runs = getSortedTable(src);
         src.close();
      }

      return new SmartSortScan(runs, tblname, comp);
   }
   
   /**
    * Returns the number of blocks in the sorted table,
    * which is the same as it would be in a
    * materialized table.
    * It does <i>not</i> include the one-time cost
    * of materializing and sorting the records.
    * @see simpledb.query.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      // does not include the one-time cost of sorting
      Plan mp = new MaterializePlan(p, tx); // not opened; just for analysis
      return mp.blocksAccessed();
   }
   
   /**
    * Returns the number of records in the sorted table,
    * which is the same as in the underlying query.
    * @see simpledb.query.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput();
   }
   
   /**
    * Returns the number of distinct field values in
    * the sorted table, which is the same as in
    * the underlying query.
    * @see simpledb.query.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }
   
   /**
    * Returns the schema of the sorted table, which
    * is the same as in the underlying query.
    * @see simpledb.query.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }

   private void performSort(SmartSortScan scan, TableInfo tblInfo, Transaction tx) {
      RecordFile record = new RecordFile(tblInfo, tx);
      record.beforeFirst();
      scan.beforeFirst();
      while(scan.next()) {
         record.next();
         for(String fldname : sch.fields()) {
            Constant c = scan.getVal(fldname);
            if(c.asJavaVal() instanceof Integer) {
               record.setInt(fldname, (Integer)c.asJavaVal());
            }
            else {
               record.setString(fldname, (String)c.asJavaVal());
            }
         }
      }
      tblInfo.sorted = sortFields.size() > 0 ? sortFields.get(0) : "";
      record.close();
      scan.close();
      SimpleDB.mdMgr().updateTableInfo(tblname, tblInfo, tx);
   }
   
   private List<TempTable> splitIntoRuns(Scan src) {
      List<TempTable> temps = new ArrayList<TempTable>();
      src.beforeFirst();
      if (!src.next())
         return temps;
      TempTable currenttemp = new TempTable(sch, tx);
      temps.add(currenttemp);
      UpdateScan currentscan = currenttemp.open();
      while (copy(src, currentscan))
         if (comp.compare(src, currentscan) < 0) {
            // start a new run
            currentscan.close();
            currenttemp = new TempTable(sch, tx);
            temps.add(currenttemp);
            currentscan = (UpdateScan) currenttemp.open();
         }
      currentscan.close();
      return temps;
   }

   private List<TempTable> getSortedTable(Scan src) {
      List<TempTable> run = new ArrayList<TempTable>();
      TempTable temp = new TempTable(sch, tx);
      src.beforeFirst();
      if(!src.next()) {
         return run;
      }
      UpdateScan scan = temp.open();
      while(copy(src, scan)) {
         //Just loop
      }
      scan.close();
      run.add(temp);
      return run;
   }
   
   private List<TempTable> doAMergeIteration(List<TempTable> runs) {
      List<TempTable> result = new ArrayList<TempTable>();
      while (runs.size() > 1) {
         TempTable p1 = runs.remove(0);
         TempTable p2 = runs.remove(0);
         result.add(mergeTwoRuns(p1, p2));
      }
      if (runs.size() == 1)
         result.add(runs.get(0));
      return result;
   }
   
   private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
      Scan src1 = p1.open();
      Scan src2 = p2.open();
      TempTable result = new TempTable(sch, tx);
      UpdateScan dest = result.open();
      
      boolean hasmore1 = src1.next();
      boolean hasmore2 = src2.next();
      while (hasmore1 && hasmore2)
         if (comp.compare(src1, src2) < 0)
         hasmore1 = copy(src1, dest);
      else
         hasmore2 = copy(src2, dest);
      
      if (hasmore1)
         while (hasmore1)
         hasmore1 = copy(src1, dest);
      else
         while (hasmore2)
         hasmore2 = copy(src2, dest);
      src1.close();
      src2.close();
      dest.close();
      return result;
   }
   
   private boolean copy(Scan src, UpdateScan dest) {
      dest.insert();
      for (String fldname : sch.fields())
         dest.setVal(fldname, src.getVal(fldname));
      return src.next();
   }
}
