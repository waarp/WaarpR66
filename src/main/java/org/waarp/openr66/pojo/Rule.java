package org.waarp.openr66.pojo;

import java.util.List;
import java.util.ArrayList;

import org.waarp.openr66.pojo.RuleTask;

/**
 * Rule data object
 */
public class Rule {

    private String name;

    private int mode;

    private List<String> hostids;

    private String recvPath;

    private String sendPath;

    private String archivePath;

    private String workPath;

    private List<RuleTask> rPreTasks;

    private List<RuleTask> rPostTasks;

    private List<RuleTask> rErrorTasks;

    private List<RuleTask> sPreTasks;

    private List<RuleTask> sPostTasks;

    private List<RuleTask> sErrorTasks;

    public Rule(String name, int mode, List<String> hostids, String recvPath,
            String sendPath, String archivePath, String workPath, 
            List<RuleTask> rPre, List<RuleTask> rPost, List<RuleTask> rError,
            List<RuleTask> sPre, List<RuleTask> sPost, List<RuleTask> sError) {
        this.name = name;
        this.mode = mode;
        this.hostids = hostids;
        this.recvPath = recvPath;
        this.sendPath = sendPath;
        this.archivePath = archivePath;
        this.workPath = workPath;
        rPreTasks = rPre;
        rPostTasks = rPost;
        rErrorTasks = rError;
        sPreTasks = sPre;
        sPostTasks = sPost;
        sErrorTasks = sError;
    }

    public Rule(String name, int mode, List<String> hostids, String recvPath,
            String sendPath, String archivePath, String workPath) {
        this(name, mode, hostids, recvPath, sendPath, archivePath, workPath,
                new ArrayList<RuleTask>(), new ArrayList<RuleTask>(),
                new ArrayList<RuleTask>(), new ArrayList<RuleTask>(),
                new ArrayList<RuleTask>(), new ArrayList<RuleTask>());
    }

    public Rule(String name, int mode, List<String> hostids) {
        this(name, mode, hostids, "", "", "", "");
    }

    public Rule(String name, int mode) {
        this(name, mode, new ArrayList<String>());
    }

    public boolean isAuthorized(String hostid) {
        return hostids.contains(hostid);
    }

    public String getXMLHostids() {
        //TODO implement getXMLHostids
        return "";
    }

    public String getXMLRPreTasks() {
        //TODO implement getXMLRPreTasks
        return "";
    }

    public String getXMLRPostTasks() {
        //TODO implement getXMLRPostTasks
        return "";
    }

    public String getXMLRErrorTasks() {
        //TODO implement getXMLRErrorTasks
        return "";
    }

    public String getXMLSPreTasks() {
        //TODO implement getXMLSPreTasks
        return "";
    }

    public String getXMLSPostTasks() {
        //TODO implement getXMLSPostTasks
        return "";
    }

    public String getXMLSErrorTasks() {
        //TODO implement getXMLSErrorTasks
        return "";
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getMode() {
        return this.mode;
    }

    public void setMode(int mode) {
        this.mode = mode;
    }

    public List<String> getHostids() {
        return this.hostids;
    }

    public void setHostids(List<String> hostids) {
        this.hostids = hostids;
    }

    public String getRecvPath() {
        return this.recvPath;
    }

    public void setRecvPath(String recvPath) {
        this.recvPath = recvPath;
    }

    public String getSendPath() {
        return this.sendPath;
    }

    public void setSendPath(String sendPath) {
        this.sendPath = sendPath;
    }

    public String getArchivePath() {
        return this.archivePath;
    }

    public void setArchivePath(String archivePath) {
        this.archivePath = archivePath;
    }

    public String getWorkPath() {
        return this.workPath;
    }

    public void setWorkPath(String workPath) {
        this.workPath = workPath;
    }

    public List<RuleTask> getRPreTasks() {
        return this.rPreTasks;
    }

    public void setRPreTasks(List<RuleTask> rPreTasks) {
        this.rPreTasks = rPreTasks;
    }

    public List<RuleTask> getRPostTasks() {
        return this.rPostTasks;
    }

    public void setRPostTasks(List<RuleTask> rPostTasks) {
        this.rPostTasks = rPostTasks;
    }

    public List<RuleTask> getRErrorTasks() {
        return this.rErrorTasks;
    }

    public void setRErrorTasks(List<RuleTask> rErrorTasks) {
        this.rErrorTasks = rErrorTasks;
    }

    public List<RuleTask> getSPreTasks() {
        return this.sPreTasks;
    }

    public void setSPreTasks(List<RuleTask> sPreTasks) {
        this.sPreTasks = sPreTasks;
    }

    public List<RuleTask> getSPostTasks() {
        return this.sPostTasks;
    }

    public void setSPostTasks(List<RuleTask> sPostTasks) {
        this.sPostTasks = sPostTasks;
    }

    public List<RuleTask> getSErrorTasks() {
        return this.sErrorTasks;
    }

    public void setSErrorTasks(List<RuleTask> sErrorTasks) {
        this.sErrorTasks = sErrorTasks;
    }
}
