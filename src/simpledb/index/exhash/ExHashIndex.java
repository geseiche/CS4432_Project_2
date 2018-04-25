package simpledb.index.exhash;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.index.Index;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
    private static int MAX_BCKT_CAP = 16; //max number of keys in each bucket
    private static String GLOBAL_TABLE = "globalTable";
    private String idxname;
    private Schema sch;
    private static Schema globalSchema; //schema for the directory
    private Transaction tx;
    private Constant searchkey = null;
    private TableScan ts = null;
    private int localDepth;
    private String bucketFileName;
    private static int staticDirectoryLength;

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
        this.globalSchema = new Schema();
        this.globalSchema.addIntField("bits");
        this.globalSchema.addStringField("filename", 20);
        this.globalSchema.addIntField("localdepth");
        TableScan tempScan = new TableScan(new TableInfo(GLOBAL_TABLE, globalSchema),tx);
        //if there are no buckets in the directory, add buckets with a depth of 1
        if(!tempScan.next()){
            tempScan.insert();
            tempScan.setInt("bits", 0);
            tempScan.setString("filename", this.idxname + 0);
            tempScan.setInt("localdepth", 1);
            tempScan.insert();
            tempScan.setInt("bits", 1);
            tempScan.setString("filename", this.idxname + 1);
            tempScan.setInt("localdepth", 1);
        }
        tempScan.close();
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
        //make global table TableInfo with Schema (bit, filename, localdepth)
        TableInfo global = new TableInfo(GLOBAL_TABLE, globalSchema);
        //new table scan on global table
        TableScan tempScan = new TableScan(global, tx);
        //find the global depth of the directory
        int gsize = 0;
        while (tempScan.next()){
            gsize++;
        }
        globalDepth = (int)(Math.log(gsize)/Math.log(2));
        int bucket = searchkey.hashCode() % (int)Math.pow(2, globalDepth);//Mask the last globalDepth bits
        //call next until you find the bit string
        //get file name and localdepth w getVal() and store
        tempScan.beforeFirst();
        while (tempScan.next()){
            if(tempScan.getInt("bits")== bucket){
                localDepth = tempScan.getInt("localdepth");
                bucketFileName = tempScan.getString("filename");
                break;
            }
        }
        tempScan.close();
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
        //get number of records in the bucket by calling next
        int size = 0;
        while (ts.next()){
            size++;
        }
        //if the bucket is full and splitting must happen
        while(size>=MAX_BCKT_CAP){
            //find the global depth
            TableScan tempScan = new TableScan(new TableInfo(GLOBAL_TABLE, globalSchema), tx);
            int gsize = 0;
            while (tempScan.next()){
                gsize++;
            }
            globalDepth = (int)(Math.log(gsize)/Math.log(2));
            //if localdepth = globaldepth then you must double the size of the directory
            if(localDepth == globalDepth){
                globalDepth++;
                ArrayList<Bucket> tempList = new ArrayList<>();
                tempScan.beforeFirst();
                //temporarily store copies of the old directory in an ArrayList of buckets
                while (tempScan.next()){
                    //note: the bits of the bucket is increased by the size of the local depth because the current buckets have a most significant bit of 0 under the new global depth
                    //   and the tempList will be used to add in the buckets with the most significant bit of 1 under the new global depth
                    tempList.add(new Bucket(tempScan.getInt("bits")+ (int)Math.pow(2, globalDepth-1), tempScan.getString("filename"), tempScan.getInt("localdepth")));
                }
                //create the new buckets from the temporary list
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
            String bucketNameA = bucketFileName; //bucket with the most significant bit of 0
            TableScan scanA = new TableScan(new TableInfo(bucketNameA,sch),tx);
            int bucketBbits = (searchkey.hashCode() % (int)Math.pow(2, localDepth-1))+ (int)Math.pow(2, localDepth-1);
            String bucketNameB = idxname + bucketBbits;//bucket with the most significant bit of 1
            TableScan scanB = new TableScan(new TableInfo(bucketNameB,sch),tx);
            while (scanA.next()){
                //if the record no longer belongs in A because it now belongs in B
                if (scanA.getVal("dataval").hashCode()%(int)Math.pow(2, localDepth)== bucketBbits){
                    //remove from A and add to B
                    scanB.insert();
                    scanB.setInt("block", scanA.getInt("block"));
                    scanB.setInt("id", scanA.getInt("id"));
                    scanB.setVal("dataval", scanA.getVal("dataval"));
                    scanA.delete();
                }
            }
            scanA.close();
            scanB.close();

            //change filenames in table to correctly point to BucketA and BucketB
            //leave all other buckets alone because their pointers do not change
            tempScan.beforeFirst();
            int mostSigBit = (int)Math.pow(2, localDepth-1);
            while (tempScan.next()){
                int bucket = tempScan.getInt("bits");
                if(bucket % mostSigBit == searchkey.hashCode() % mostSigBit){
                    tempScan.setInt("localdepth", localDepth);
                    if(bucket/mostSigBit>=1){
                        tempScan.setString("filename", bucketNameB);
                    } else {
                        tempScan.setString("filename", bucketNameA);
                    }
                }
            }
            tempScan.close();

            //set ts to new bucket
            beforeFirst(val);

            //get length by calling next
            size = 0;
            while (ts.next()){
                size++;
            }
        }
        //insert the new index record
        ts.insert();
        ts.setInt("block", rid.blockNumber());
        ts.setInt("id", rid.id());
        ts.setVal("dataval", val);

        //print out the size of the global table
        TableScan tempScan = new TableScan(new TableInfo(GLOBAL_TABLE, globalSchema), tx);
        int gsize = 0;
        while (tempScan.next()){
            gsize++;
        }
        System.out.println("Global Directory Size: " + gsize);
        tempScan.close();

        //for debugging only; this writes the index to a file each time the index is updated
        /*
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("output.txt", true));
            writer.append(this.toString());
            writer.close();
        } catch (IOException e){
            e.printStackTrace();
        }
        */

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
    //TODO
    //SOS HELP
    public static int searchCost(int numblocks, int rpb){
        int globalTableSize = -1; //HARDCODE THIS
        return globalTableSize/rpb + MAX_BCKT_CAP;

    }

    @Override
    public String toString() {
        String finalString = "";
        TableScan scan = new TableScan(new TableInfo(GLOBAL_TABLE, globalSchema), tx);
        //scans the directory
        while (scan.next()){
            finalString = finalString + "\n" + Integer.toBinaryString(scan.getInt("bits"));
            TableScan innerscan = new TableScan(new TableInfo(scan.getString("filename"), sch), tx);
            //scan each bucket
            while (innerscan.next()){
                finalString = finalString + "\n\t" + innerscan.getVal("dataval").hashCode() + "\t" + Integer.toBinaryString(innerscan.getVal("dataval").hashCode());
            }
            innerscan.close();
        }
        scan.close();
        return finalString + "\n----------------\n";
    }
}
