package simpledb.index.exhash;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.index.Index;

import java.util.HashMap;

//CS4432:Added entire class to implement extensible hashing

/**
 * A static hash implementation of the Index interface.
 * A fixed number of buckets is allocated (currently, 100),
 * and each bucket is implemented as a file of index records.
 * @author Edward Sciore
 */
public class ExHashIndex implements Index {
    private int NUM_BUCKETS = 4; //number of buckets in the directory
    private int globalDepth = 2; //global depth of the directory
    private static int MAX_BCKT_CAP = 8; //max number of keys in each bucket
    private HashMap<Integer, Bucket> buckets = new HashMap<>(); //list of all the buckets that the directory points to
    private String idxname;
    private Schema sch;
    private Transaction tx;
    private Constant searchkey = null;
    private TableScan ts = null;

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
        String tblname = idxname + bucket;
        TableInfo ti = new TableInfo(tblname, sch);
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
