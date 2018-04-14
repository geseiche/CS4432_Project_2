package simpledb.index.exhash;

import java.util.ArrayList;

public class Bucket {
    private int local_depth;
    //Arraylists

    public Bucket(int local_depth) {
        this.local_depth = local_depth;
    }

    public int getLocal_depth() {
        return local_depth;
    }

    public void setLocal_depth(int local_depth) {
        this.local_depth = local_depth;
    }
}
