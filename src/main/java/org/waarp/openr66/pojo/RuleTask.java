package org.waarp.openr66.pojo;

/**
 * RuleTask data object
 */
public class RuleTask {

    private String type;

    private String path;

    private int delay;

    public RuleTask(String type, String path, int delay) {
        this.type = type;
        this.path = path;
        this.delay = delay;
    }

    public String getXML() {
        String res = "<task>";
        res = res + "<type>" + type + "</type>";
        res = res + "<path>" + path + "</path>";
        res = res + "<delay>" + delay + "</delay>";
        return res + "</task>";
    }

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPath() {
        return this.path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getDelay() {
        return this.delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }
}
