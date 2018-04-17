package simpledb.index.exhash;

public class Bucket {
    private int bits;
    private String fileName;
    private int localDepth;

    public Bucket(int bits, String fileName, int localDepth) {
        this.bits = bits;
        this.fileName = fileName;
        this.localDepth = localDepth;
    }

    public int getBits() {
        return bits;
    }

    public void setBits(int bits) {
        this.bits = bits;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getLocalDepth() {
        return localDepth;
    }

    public void setLocalDepth(int localDepth) {
        this.localDepth = localDepth;
    }
}
