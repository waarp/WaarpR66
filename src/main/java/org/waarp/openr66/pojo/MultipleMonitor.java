package org.waarp.openr66.pojo;

/**
 * MultipleMonitor data object
 */
public class MultipleMonitor {

    private String hostid;

    private int countConfig;

    private int countHost;

    private int countRule;

    public MultipleMonitor(String hostid, int countConfig, int countHost,
            int countRule) {
        this.hostid = hostid;
        this.countConfig = countConfig;
        this.countHost = countHost;
        this.countRule = countRule;
    }

    public DataError validate() {
        DataError res = new DataError();
        //Tests
        if (countConfig < 0) {
            res.add("countConfig", "Count must be positive or null");
        }
        if (countHost < 0) {
            res.add("countHost", "Count must be positive or null");
        }
        if (countRule < 0) {
            res.add("countRule", "Count must be positive or null");
        }
        return res;
    }

    public String getHostid() {
        return this.hostid;
    }

    public void setHostid(String hostid) {
        this.hostid = hostid;
    }

    public int getCountConfig() {
        return this.countConfig;
    }

    public void setCountConfig(int countConfig) {
        this.countConfig = countConfig;
    }

    public int getCountHost() {
        return this.countHost;
    }

    public void setCountHost(int countHost) {
        this.countHost = countHost;
    }

    public int getCountRule() {
        return this.countRule;
    }

    public void setCountRule(int countRule) {
        this.countRule = countRule;
    }
}
