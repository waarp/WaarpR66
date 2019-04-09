package org.waarp.openr66.pojo;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;

import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;

/**
 * Transfer data object
 */
public class Transfer {

    public enum TASKSTEP {
        NOTASK(0), PRETASK(1), TRANSFERTASK(2), POSTTASK(3), ALLDONETASK(4),
        ERRORTASK(5);

        private int taskNo;

        private static Map<Integer, TASKSTEP> map 
            = new HashMap<Integer, TASKSTEP>();

        static {
            for (TASKSTEP task : TASKSTEP.values()) {
                map.put(task.taskNo, task);
            }
        }

        TASKSTEP(final int task) {
            taskNo = task;
        }

        public static TASKSTEP valueOf(int taskStep) {
            return map.get(taskStep);
        }

        public DbTaskRunner.TASKSTEP toLegacy() {
            return DbTaskRunner.TASKSTEP.valueOf(this.name());
        }
    }

    private long id = DbConstant.ILLEGALVALUE;

    /**
     * True if requester is the sender of the file (SEND MODE)
     * False if requested is the sender of the file (RETRIEVE MODE)
     */
    private boolean retrieveMode = false;

    private String rule = "";

    private int transferMode = 1;

    private String filename = "";

    private String originalName = "";

    private String fileInfo = "";

    private boolean isFileMoved = false;

    private int blockSize;

    private String ownerRequest = Configuration.configuration.getHOST_ID();

    private String requester = "";

    private String requested = "";

    private String transferInfo = "";

    private TASKSTEP globalStep = TASKSTEP.NOTASK;

    private TASKSTEP lastGlobalStep = TASKSTEP.NOTASK;

    private int step = -1;

    private ErrorCode stepStatus = ErrorCode.Unknown;

    private ErrorCode infoStatus = ErrorCode.Unknown;

    private int rank = 0;

    private Timestamp start = new Timestamp(0);

    private Timestamp stop = new Timestamp(0);

    private UpdatedInfo updatedInfo = UpdatedInfo.UNKNOWN;

    /**
     * Full Constructor to create Transfer from the database
     * @param id
     * @param rule
     * @param mode
     * @param filename
     * @param originalName
     * @param fileInfo
     * @param isFileMoved
     * @param blockSize
     * @param retrieveMode
     * @param ownerReq
     * @param requester
     * @param requested
     * @param transferInfo
     * @param globalStep
     * @param lastGlobalStep
     * @param step
     * @param stepStatus
     * @param infoStatus
     * @param rank
     * @param start
     * @param stop
     * @param updatedInfo
     */
    public Transfer(long id, String rule, int mode, String filename,
            String originalName, String fileInfo, boolean isFileMoved,
            int blockSize, boolean retrieveMode, String ownerReq, String requester,
            String requested, String transferInfo,TASKSTEP globalStep,
            TASKSTEP lastGlobalStep, int step, ErrorCode stepStatus,
            ErrorCode infoStatus, int rank, Timestamp start, Timestamp stop,
            UpdatedInfo updatedInfo) {
        this (id, rule, mode, filename, originalName, fileInfo, isFileMoved,
                blockSize, retrieveMode, ownerReq, requester, requested, transferInfo,
                globalStep, lastGlobalStep, step, stepStatus, infoStatus, rank,
                start, stop);
        this.updatedInfo = updatedInfo;
    }

    /**
     * Constructor to create Transfer from remote requests
     * @param id
     * @param rule
     * @param mode
     * @param filename
     * @param originalName
     * @param fileInfo
     * @param isFileMoved
     * @param blockSize
     * @param retrieveMode
     * @param ownerReq
     * @param requester
     * @param requested
     * @param transferInfo
     * @param globalStep
     * @param lastGlobalStep
     * @param step
     * @param stepStatus
     * @param infoStatus
     * @param rank
     * @param start
     * @param stop
     */
    public Transfer(long id, String rule, int mode, String filename,
            String originalName, String fileInfo, boolean isFileMoved,
            int blockSize, boolean retrieveMode, String ownerReq, String requester,
            String requested, String transferInfo,TASKSTEP globalStep,
            TASKSTEP lastGlobalStep, int step, ErrorCode stepStatus,
            ErrorCode infoStatus, int rank, Timestamp start, Timestamp stop) {
        this.id = id;
        this.rule = rule;
        this.transferMode = mode;
        this.retrieveMode = retrieveMode;
        this.filename = filename;
        this.originalName = originalName;
        this.fileInfo = fileInfo;
        this.isFileMoved = isFileMoved;
        this.blockSize = blockSize;
        this.ownerRequest = ownerReq;
        this.requester = requester;
        this.requested = requested;
        this.transferInfo = transferInfo;
        this.globalStep = globalStep;
        this.lastGlobalStep = lastGlobalStep;
        this.step = step;
        this.stepStatus = stepStatus;
        this.infoStatus = infoStatus;
        this.rank = rank;
        this.start = start;
        this.stop = stop;
    }

    /**
     * Constructor to create transfer locally with delayed start time
     *
     *
     * @param rule
     * @param retrieveMode
     * @param file
     * @param fileInfo
     * @param blockSize
     */
    public Transfer(String remote, String rule, int ruleMode,
                    boolean retrieveMode, String file, String fileInfo,
                    int blockSize, Timestamp start) {
        this.ownerRequest = Configuration.configuration.getHOST_ID();
        this.requester = Configuration.configuration.getHOST_ID();
        this.requester = remote;
        this.rule = rule;
        this.transferMode = ruleMode;
        this.retrieveMode = retrieveMode;
        this.filename = file;
        this.originalName = file;
        this.fileInfo = fileInfo;
        this.blockSize = blockSize;
        this.start = start;
    }

    /**
     * Constructor to create transfer locally
     *
     * @param rule
     * @param retrieveMode
     * @param file
     * @param fileInfo
     * @param blockSize
     */
    public Transfer(String remote, String rule, int ruleMode, boolean retrieveMode, String file,
            String fileInfo, int blockSize) {
        this (remote, rule, ruleMode, retrieveMode, file, fileInfo, blockSize,
                new Timestamp(new Date().getTime()));
    }

    /**
     * Empty constructor for compatibility issues
     */
    @Deprecated
    public Transfer() {}

    public long getId() {
        return this.id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public boolean getRetrieveMode() {
        return this.retrieveMode;
    }

    public void setRetrieveMode(boolean retrieveMode) {
        this.retrieveMode = retrieveMode;
    }

    public String getRule() {
        return this.rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

    public int getTransferMode() {
        return this.transferMode;
    }

    public void setTransferMode(int mode) {
        this.transferMode = mode;
    }

    public String getFilename() {
        return this.filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getOriginalName() {
        return this.originalName;
    }

    public void setOriginalName(String originalName) {
        this.originalName = originalName;
    }

    public String getFileInfo() {
        return this.fileInfo;
    }

    public void setFileInfo(String fileInfo) {
        this.fileInfo = fileInfo;
    }

    public boolean getIsMoved() {
        return this.isFileMoved;
    }

    public void setIsMoved(boolean isFileMoved) {
        this.isFileMoved = isFileMoved;
    }

    public int getBlockSize() {
        return this.blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public String getOwnerRequest() {
        return this.ownerRequest;
    }

    public void setOwnerRequest(String ownerRequest) {
        this.ownerRequest = ownerRequest;
    }

    public String getRequester() {
        return this.requester;
    }

    public void setRequester(String requester) {
        this.requester = requester;
    }

    public String getRequested() {
        return this.requested;
    }

    public void setRequested(String requested) {
        this.requested = requested;
    }

    public String getTransferInfo() {
        return this.transferInfo;
    }

    public void setTransferInfo(String transferInfo) {
        this.transferInfo = transferInfo;
    }

    public TASKSTEP getGlobalStep() {
        return this.globalStep;
    }

    public void setGlobalStep(TASKSTEP globalStep) {
        this.globalStep = globalStep;
    }
    public TASKSTEP getLastGlobalStep() {
        return this.lastGlobalStep;
    }

    public void setLastGlobalStep(TASKSTEP lastGlobalStep) {
        this.lastGlobalStep = lastGlobalStep;
    }

    public int getStep() {
        return this.step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public ErrorCode getStepStatus() {
        return this.stepStatus;
    }

    public void setStepStatus(ErrorCode stepStatus) {
        this.stepStatus = stepStatus;
    }

    public ErrorCode getInfoStatus() {
        return this.infoStatus;
    }

    public void setInfoStatus(ErrorCode infoStatus) {
        this.infoStatus = infoStatus;
    }

    public int getRank() {
        return this.rank;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    public Timestamp getStart() {
        return this.start;
    }

    public void setStart(Timestamp start) {
        this.start = start;
    }

    public Timestamp getStop() {
        return this.stop;
    }

    public void setStop(Timestamp stop) {
        this.stop = stop;
    }

    public UpdatedInfo getUpdatedInfo() {
        return this.updatedInfo;
    }

    public void setUpdatedInfo(UpdatedInfo info) {
        this.updatedInfo = info;
    }
}
