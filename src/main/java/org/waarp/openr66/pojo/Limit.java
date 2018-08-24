package org.waarp.openr66.pojo;

/**
 * Limit data object
 */
public class Limit {

    private String hostid;

    private long readGlobalLimit;

    private long writeGlobalLimit;

    private long readSessionLimit;

    private long writeSessionLimit;

    private long delayLimit;

    private int updatedInfo = 0;

    public Limit(String hostid, long delayLimit, long readGlobalLimit,
            long writeGlobalLimit, long readSessionLimit,
            long writeSessionLimit, int updatedInfo) {
        this(hostid, delayLimit, readGlobalLimit, writeGlobalLimit, 
                readSessionLimit, writeSessionLimit);
        this.updatedInfo = updatedInfo;
    }

    public Limit(String hostid, long delayLimit, long readGlobalLimit,
            long writeGlobalLimit, long readSessionLimit,
            long writeSessionLimit) {
        this.hostid = hostid;
        this.delayLimit = delayLimit;	
        this.readGlobalLimit = readGlobalLimit;
        this.writeGlobalLimit = writeGlobalLimit;
        this.readSessionLimit = readSessionLimit;
        this.writeSessionLimit = writeSessionLimit;
    }

    public Limit(String hostid, long delayLimit) {
        this(hostid, delayLimit, 0, 0, 0, 0);
    }

    public DataError validate() {
        DataError res = new DataError();
        //Tests
        if (readGlobalLimit < 0) {
            res.add("readGlobalLimit", "Limit must be positive or null");
        }
        if (writeGlobalLimit < 0) {
            res.add("writeGlobalLimit", "Limit must be positive or null");
        }
        if (readSessionLimit < 0) {
            res.add("readSessionLimit", "Limit must be positive or null");
        }
        if (writeSessionLimit < 0) {
            res.add("writeSessionLimit", "Limit must be positive or null");
        }
        if (delayLimit < 0) {
            res.add("delayLimit", "Limit must be positive or null");
        }
        return res;
    }

    public String getHostid() {
        return this.hostid;
    }

    public void setHostid(String hostid) {
        this.hostid = hostid;
    }

    public long getReadGlobalLimit() {
        return this.readGlobalLimit;
    }

    public void setReadGlobalLimit(long readGlobalLimit) {
        this.readGlobalLimit = readGlobalLimit;
    }

    public long getWriteGlobalLimit() {
        return this.writeGlobalLimit;
    }

    public void setWriteGlobalLimit(long writeGlobalLimit) {
        this.writeGlobalLimit = writeGlobalLimit;
    }

    public long getReadSessionLimit() {
        return this.readSessionLimit;
    }

    public void setReadSessionLimit(long readSessionLimit) {
        this.readSessionLimit = readSessionLimit;
    }

    public long getWriteSessionLimit() {
        return this.writeSessionLimit;
    }

    public void setWriteSessionLimit(long writeSessionLimit) {
        this.writeSessionLimit = writeSessionLimit;
    }

    public long getDelayLimit() {
        return this.delayLimit;
    }

    public void setDelayLimit(long delayLimit) {
        this.delayLimit = delayLimit;
    }

    public int getUpdatedInfo() {
        return this.updatedInfo;
    }

    public void setUpdatedInfo(int info) {
        this.updatedInfo = info;
    }
}
