/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License, or (at your option)
 * any later version.
 *
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package openr66.task;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.filesystem.R66Rule;
import openr66.filesystem.R66Session;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.utils.R66Future;
import openr66.task.exception.OpenR66RunnerEndTasksException;
import openr66.task.exception.OpenR66RunnerErrorException;

/**
 * @author Frederic Bregier
 *
 */
public class TaskRunner {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(TaskRunner.class);

    public static final int NOTASK = -1;

    public static final int PRETASK = 0;

    public static final int POSTTASK = 1;

    public static final int ERRORTASK = 2;

    public static final int TRANSFERTASK = 3;

    public static final int ALLDONETASK = 4;

    public static enum TaskStatus {
        UNKNOWN, RUNNING, OK, ERROR;
    }

    private final R66Rule rule;

    private final R66Session session;

    private int globalstep = NOTASK;

    private int step = NOTASK;

    private int rank = 0;

    private TaskStatus status;

    // FIXME need a special ID
    private final long specialId;

    private final boolean isRetrieve;

    private String filename;

    private boolean isFileMoved = false;

    private final int blocksize;

    private String originalFilename;

    private final String fileInformation;

    private final int mode;


    /**
     * @param rule
     * @param session
     * @param globalstep
     * @param step
     * @param rank
     * @param status
     * @param specialId
     * @param isRetrieve
     * @param filename
     * @param isFileMoved
     * @param blocksize
     * @param originalFilename
     * @param fileInformation
     * @param mode
     */
    public TaskRunner(R66Rule rule, R66Session session, int globalstep,
            int step, int rank, TaskStatus status, long specialId,
            boolean isRetrieve, String filename, boolean isFileMoved,
            int blocksize, String originalFilename, String fileInformation,
            int mode) {
        this.rule = rule;
        this.session = session;
        this.globalstep = globalstep;
        this.step = step;
        this.rank = rank;
        this.status = status;
        this.specialId = specialId;
        this.isRetrieve = isRetrieve;
        this.filename = filename;
        this.isFileMoved = isFileMoved;
        this.blocksize = blocksize;
        this.originalFilename = originalFilename;
        this.fileInformation = fileInformation;
        this.mode = mode;
    }

    public TaskRunner(R66Session session, R66Rule rule, boolean isRetrieve,
            RequestPacket requestPacket) {
        this.session = session;
        this.rule = rule;
        status = TaskStatus.UNKNOWN;
        this.blocksize = requestPacket.getBlocksize();
        this.originalFilename = requestPacket.getFilename();
        this.fileInformation = requestPacket.getFileInformation();
        this.mode = requestPacket.getMode();
        long newId = this.session.getLocalChannelReference().getRemoteId();
        newId = newId << 32;
        // FIXME need a way to check if it does not already exist
        specialId = newId +
                this.session.getLocalChannelReference().getLocalId();
        this.isRetrieve = isRetrieve;
    }

    public TaskRunner(R66Session session, R66Rule rule, long id) {
        this.session = session;
        this.rule = rule;
        status = TaskStatus.UNKNOWN;
        specialId = id;
        // FIXME load from database
        this.blocksize = this.session.getRequest().getBlocksize();
        this.originalFilename = this.session.getRequest().getFilename();
        this.fileInformation = this.session.getRequest().getFileInformation();
        this.mode = this.session.getRequest().getMode();
        isRetrieve = false;// XXX FIXME TODO WARNING FALSE!!!
    }

    /**
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * @param filename
     *            the filename to set
     */
    public void setFilename(String filename) {
        this.filename = filename;
    }

    /**
     * @return the specialId
     */
    public long getSpecialId() {
        return specialId;
    }

    /**
     * @return the rule
     */
    public R66Rule getRule() {
        return rule;
    }

    /**
     * @return the status
     */
    public TaskStatus getStatus() {
        return status;
    }

    public boolean isRetrieve() {
        return isRetrieve;
    }

    public void setPreTask(int step) {
        globalstep = PRETASK;
        this.step = step;
        status = TaskStatus.RUNNING;
    }

    public void setTransferTask(int rank) {
        globalstep = TRANSFERTASK;
        this.rank = rank;
        status = TaskStatus.RUNNING;
    }

    public int finishTransferTask(boolean status) {
        if (status) {
            this.status = TaskStatus.OK;
        } else {
            this.status = TaskStatus.ERROR;
        }
        return rank;
    }

    public void setPostTask(int step) {
        globalstep = POSTTASK;
        this.step = step;
        status = TaskStatus.RUNNING;
    }

    public void setErrorTask(int step) {
        globalstep = ERRORTASK;
        this.step = step;
        status = TaskStatus.RUNNING;
    }

    public void setAllDone() {
        globalstep = ALLDONETASK;
        step = 0;
        status = TaskStatus.OK;
    }

    private R66Future runNextTask(String[][] tasks)
            throws OpenR66RunnerEndTasksException, OpenR66RunnerErrorException {
        if (tasks == null) {
            throw new OpenR66RunnerEndTasksException("No tasks!");
        }
        if (tasks.length <= step) {
            throw new OpenR66RunnerEndTasksException();
        }
        String name = tasks[step][0];
        String arg = tasks[step][1];
        AbstractTask task = TaskFactory.getTaskFromId(name, arg, session);
        task.run();
        task.futureCompletion.awaitUninterruptibly();
        return task.futureCompletion;
    }

    private R66Future runNext() throws OpenR66RunnerEndTasksException,
            OpenR66RunnerErrorException {
        switch (globalstep) {
            case PRETASK:
                return runNextTask(rule.preTasksArray);
            case POSTTASK:
                return runNextTask(rule.postTasksArray);
            case ERRORTASK:
                return runNextTask(rule.errorTasksArray);
            default:
                throw new OpenR66RunnerErrorException("Global Step unknown");
        }
    }

    public void run() throws OpenR66RunnerErrorException {
        R66Future future;
        if (this.status != TaskStatus.RUNNING) {
            throw new OpenR66RunnerErrorException("Current global STEP not ready to run");
        }
        while (true) {
            try {
                future = runNext();
            } catch (OpenR66RunnerEndTasksException e) {
                status = TaskStatus.OK;
                return;
            }
            if (future.isCancelled()) {
                status = TaskStatus.ERROR;
                throw new OpenR66RunnerErrorException("Runner is error: " +
                        future.getCause().getMessage(), future.getCause());
            }
            step ++;
        }
    }

    public boolean ready() {
        return globalstep != NOTASK;
    }

    public boolean isFinished() {
        return (globalstep != ALLDONETASK) || (globalstep == ERRORTASK && status == TaskStatus.OK);
    }
    /**
     * @return the rank
     */
    public int getRank() {
        return rank;
    }

    /**
     * Increment the rank
     */
    public void incrementRank() {
        rank ++;
    }

    /**
     * @return the isFileMoved
     */
    public boolean isFileMoved() {
        return isFileMoved;
    }

    /**
     * @param isFileMoved
     *            the isFileMoved to set
     */
    public void setFileMoved(boolean isFileMoved) {
        this.isFileMoved = isFileMoved;
    }

    /**
     * @return the originalFilename
     */
    public String getOriginalFilename() {
        return originalFilename;
    }

    /**
     * @param originalFilename the originalFilename to set
     */
    public void setOriginalFilename(String originalFilename) {
        this.originalFilename = originalFilename;
    }

    /**
     * @return the blocksize
     */
    public int getBlocksize() {
        return blocksize;
    }

    /**
     * @return the fileInformation
     */
    public String getFileInformation() {
        return fileInformation;
    }

    /**
     * @return the mode
     */
    public int getMode() {
        return mode;
    }

    /**
     * This method is to be called each time an operation is happening on Runner
     */
    public void saveStatus() {
        // FIXME should save status to DB
        // FIXME need a specialID that could be reused over time
        // save: rulename, GLOBALSTEP, setp, RANK, status, specialId, FILENAME,
        // isRetrieve
        logger.info(GgInternalLogger.getRankMethodAndLine(3) + " " +
                toString());
    }

    public void clear() {

    }

    @Override
    public String toString() {
        return "Run: " + (rule != null? rule.toString() : "no Rule") + " on " +
                filename + " STEP: " + globalstep + ":" + step + ":" + status +
                ":" + rank + " SpecialId: " + specialId + " isRetr: " +
                isRetrieve + " isMoved: " + isFileMoved;
    }
}
