package simpledb.index.exhash;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.index.Index;

import java.util.ArrayList;
import java.util.HashMap;

//CS4432:Added entire class to implement extensible hashing

/**
 * A static hash implementation of the Index interface.
 * A fixed number of buckets is allocated (currently, 100),
 * and each bucket is implemented as a file of index records.
 * @author Edward Sciore
 */
public class ExHashIndex implements Index {
    private int NUM_BUCKETS = 1; //number of buckets in the directory
    private int globalDepth = 1; //global depth of the directory
    private static int MAX_BCKT_CAP = 4; //max number of keys in each bucket
    private static String GLOBAL_TABLE = "globalTable";
    private String idxname;
    private Schema sch;
    private Schema globalSchema; //schema for the directory
    private Transaction tx;
    private Constant searchkey = null;
    private TableScan ts = null;
    private int localDepth;
    private String bucketFileName;

    /**
     * Opens a hash index for the specified index.
     * @param idxname the name of the index
     * @param sch the schema of the index records
     * @param tx the calling transaction
     */
    public ExHashIndex(String idxname, Schema sch, Transaction tx) {
        this.idxname = idxname;
        this.sch = sch;
        this.tx = tx;
        this.globalSchema.addIntField("bits");
        this.globalSchema.addStringField("filename", 20);
        this.globalSchema.addIntField("localdepth");
        TableScan tempScan = new TableScan(new TableInfo(GLOBAL_TABLE, globalSchema),tx);
        tempScan.insert();
        tempScan.setInt("bits", 0);
        tempScan.setString("filename", this.idxname + 0);
        tempScan.setInt("localdepth", 1);
        tempScan.insert();
        tempScan.setInt("bits", 1);
        tempScan.setString("filename", this.idxname + 1);
        tempScan.setInt("localdepth", 1);
    }

    /**
     * Positions the index before the first index record
     * having the specified search key.
     * The method hashes the search key to determine the bucket,
     * and then opens a table scan on the file
     * corresponding to the bucket.
     * The table scan for the previous bucket (if any) is closed.
     * @see simpledb.index.Index#beforeFirst(simpledb.query.Constant)
     */
    public void beforeFirst(Constant searchkey) {
        close();
        this.searchkey = searchkey;
        int bucket = searchkey.hashCode() % (int)Math.pow(2, globalDepth);//Mask the last global_length bits
        //TODO
        //make global table TAbleInfo w Schema (bit, filename, localdepth)
        TableInfo global = new TableInfo(GLOBAL_TABLE, globalSchema);
        //new table scan on global table
        TableScan tempScan = new TableScan(global, tx);
        //call next until you find the bit string
        //get file name and localdepth w getVal() and store
        while (tempScan.next()){
            if(tempScan.getInt("bits")== bucket){
                localDepth = tempScan.getInt("localdepth");
                bucketFileName = tempScan.getString("filename");
                break;
            }
        }
        //use the bucket table name in the given ti
        String tblname = idxname + bucket; //do I even still need this?
        TableInfo ti = new TableInfo(bucketFileName, sch);
        ts = new TableScan(ti, tx);
    }

    /**
     * Moves to the next record having the search key.
     * The method loops through the table scan for the bucket,
     * looking for a matching record, and returning false
     * if there are no more such records.
     * @see simpledb.index.Index#next()
     */
    public boolean next() {
        while (ts.next())
            if (ts.getVal("dataval").equals(searchkey))
                return true;
        return false;
    }

    /**
     * Retrieves the dataRID from the current record
     * in the table scan for the bucket.
     * @see simpledb.index.Index#getDataRid()
     */
    public RID getDataRid() {
        int blknum = ts.getInt("block");
        int id = ts.getInt("id");
        return new RID(blknum, id);
    }

    /**
     * Inserts a new record into the table scan for the bucket.
     * @see simpledb.index.Index#insert(simpledb.query.Constant, simpledb.record.RID)
     */
    public void insert(Constant val, RID rid) {
        beforeFirst(val);
        //TODO check if bucket is full or needs to be split, if so, split; where does the local depth go?
        //get length by calling next
        int size = 0;
        while (ts.next()){
            size++;
        }
        while(size>=MAX_BCKT_CAP){
            TableScan tempScan = new TableScan(new TableInfo(GLOBAL_TABLE, globalSchema), tx);
            //if localdepth = globaldepth
            if(localDepth == globalDepth){
                //read global file
                //delete all the records
                //re-insert with longer bit string
                globalDepth++;
                ArrayList<Bucket> tempList = new ArrayList<>();
                while (tempScan.next()){
                    tempList.add(new Bucket(tempScan.getInt("bits")+ (int)Math.pow(2, globalDepth-1), tempScan.getString("filename"), tempScan.getInt("localdepth")));
                }
                for (Bucket b : tempList){
                    tempScan.insert();
                    tempScan.setInt("bits", b.getBits());
                    tempScan.setString("filename", b.getFileName());
                    tempScan.setInt("localdepth", b.getLocalDepth());
                }
            }

            //increase local depth
            localDepth++;

            //split the bucket
            String bucketNameA = bucketFileName;
            TableScan scanA = new TableScan(new TableInfo(bucketNameA,sch),tx);
            int bucketBbits = (searchkey.hashCode() % (int)Math.pow(2, localDepth-1))+ (int)Math.pow(2, localDepth-1);
            String bucketNameB = idxname + bucketBbits;
            TableScan scanB = new TableScan(new TableInfo(bucketNameB,sch),tx);
            while (scanA.next()){
                if (scanA.getVal("dataval").hashCode()%(int)Math.pow(2, localDepth)== bucketBbits){
                    //remove from A and add to B
                    scanB.insert();
                    scanB.setInt("block", scanA.getInt("block"));
                    scanB.setInt("id", scanA.getInt("id"));
                    scanB.setVal("dataval", scanA.getVal("dataval"));
                    scanA.delete();
                }
            }

            //change filenames in table
            tempScan.beforeFirst();
            int mostSigBit = (int)Math.pow(2, localDepth-1);
            while (tempScan.next()){
                int bucket = tempScan.getInt("bits");
                if(bucket % mostSigBit == searchkey.hashCode() % mostSigBit){
                    if(bucket/mostSigBit>=1){
                        tempScan.setString("filename", bucketNameB);
                    } else {
                        tempScan.setString("filename", bucketNameA);
                    }
                }
            }

            //set ts to new bucket
            beforeFirst(val);

            //get length by calling next
            size = 0;
            while (ts.next()){
                size++;
            }
        }
        ts.insert();
        ts.setInt("block", rid.blockNumber());
        ts.setInt("id", rid.id());
        ts.setVal("dataval", val);
    }

    /**
     * Deletes the specified record from the table scan for
     * the bucket.  The method starts at the beginning of the
     * scan, and loops through the records until the
     * specified record is found.
     * @see simpledb.index.Index#delete(simpledb.query.Constant, simpledb.record.RID)
     */
    public void delete(Constant val, RID rid) {
        beforeFirst(val);
        while(next())
            if (getDataRid().equals(rid)) {
                ts.delete();
                return;
            }
    }

    /**
     * Closes the index by closing the current table scan.
     * @see simpledb.index.Index#close()
     */
    public void close() {
        if (ts != null)
            ts.close();
    }

    /**
     * Returns the cost of searching an index file having the
     * specified number of blocks.
     * The method assumes that all buckets are about the
     * same size, and so the cost is simply the size of
     * the bucket.
     * @param numblocks the number of blocks of index records
     * @param rpb the number of records per block (not used here)
     * @return the cost of traversing the index
     */
    public int searchCost(int numblocks, int rpb){
        return numblocks / NUM_BUCKETS;
    }
}
