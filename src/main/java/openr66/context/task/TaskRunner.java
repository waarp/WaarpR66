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
package openr66.context.task;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.context.R66Rule;
import openr66.context.R66Session;
import openr66.context.task.exception.OpenR66RunnerEndTasksException;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.data.DbTaskRunner;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.protocol.config.Configuration;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.utils.R66Future;

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

    private final long specialId;

    private final boolean isRetrieve;

    private String filename;

    private boolean isFileMoved = false;

    private final int blocksize;

    private String originalFilename;

    private final String fileInformation;

    private final int mode;

    private final String requesterHostId;

    private final String requestedHostId;

    private final DbTaskRunner internalDbTaskRunner;


    public TaskRunner(R66Session session, R66Rule rule, boolean isRetrieve,
            RequestPacket requestPacket) throws OpenR66DatabaseException {
        this.session = session;
        this.rule = rule;
        this.rank = requestPacket.getRank();
        this.status = TaskStatus.UNKNOWN;
        this.isRetrieve = isRetrieve;
        this.filename = requestPacket.getFilename();
        this.blocksize = requestPacket.getBlocksize();
        this.originalFilename = requestPacket.getFilename();
        this.fileInformation = requestPacket.getFileInformation();
        this.mode = requestPacket.getMode();
        if (requestPacket.isToValidate()) {
            this.requesterHostId = session.getAuth().getUser();
            this.requestedHostId = Configuration.configuration.HOST_ID;
        } else {
            this.requestedHostId = session.getAuth().getUser();
            this.requesterHostId = Configuration.configuration.HOST_ID;
        }

        this.internalDbTaskRunner =
            new DbTaskRunner(requestPacket.getSpecialId(),
                this.globalstep, this.step, this.rank, this.status, this.isRetrieve,
                this.filename, this.isFileMoved, this.rule.idRule, this.blocksize,
                this.originalFilename, this.fileInformation, this.mode,
                this.requesterHostId, this.requestedHostId);
        internalDbTaskRunner.insert();

        specialId = internalDbTaskRunner.getSpecialId();
    }

    public TaskRunner(R66Session session, R66Rule rule, long id) throws OpenR66DatabaseException {
        this.session = session;
        this.rule = rule;
        specialId = id;

        this.internalDbTaskRunner =
            new DbTaskRunner(id);
        internalDbTaskRunner.select();

        globalstep = internalDbTaskRunner.getGlobalstep();
        step = internalDbTaskRunner.getStep();
        rank = internalDbTaskRunner.getRank();
        status = internalDbTaskRunner.getStatus();
        isRetrieve = internalDbTaskRunner.isRetrieve();
        filename = internalDbTaskRunner.getFilename();
        isFileMoved = internalDbTaskRunner.isFileMoved();
        blocksize = internalDbTaskRunner.getBlocksize();
        originalFilename = internalDbTaskRunner.getOriginalFilename();
        fileInformation = internalDbTaskRunner.getFileInformation();
        mode = internalDbTaskRunner.getMode();
        requesterHostId = internalDbTaskRunner.getRequesterHostId();
        requestedHostId = internalDbTaskRunner.getRequestedHostId();
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
        this.internalDbTaskRunner.setFilename(filename);
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
        this.internalDbTaskRunner.setGlobalstep(globalstep);
        this.internalDbTaskRunner.setStep(this.step);
        this.internalDbTaskRunner.setStatus(status);
    }

    public void setTransferTask(int rank) {
        globalstep = TRANSFERTASK;
        this.rank = rank;
        status = TaskStatus.RUNNING;
        this.internalDbTaskRunner.setGlobalstep(globalstep);
        this.internalDbTaskRunner.setRank(this.rank);
        this.internalDbTaskRunner.setStatus(status);
    }

    public int finishTransferTask(boolean status) {
        if (status) {
            this.status = TaskStatus.OK;
        } else {
            this.status = TaskStatus.ERROR;
        }
        this.internalDbTaskRunner.setStatus(this.status);
        return rank;
    }

    public void setPostTask(int step) {
        globalstep = POSTTASK;
        this.step = step;
        status = TaskStatus.RUNNING;
        this.internalDbTaskRunner.setGlobalstep(globalstep);
        this.internalDbTaskRunner.setStep(this.step);
        this.internalDbTaskRunner.setStatus(status);
    }

    public void setErrorTask(int step) {
        globalstep = ERRORTASK;
        this.step = step;
        status = TaskStatus.RUNNING;
        this.internalDbTaskRunner.setGlobalstep(globalstep);
        this.internalDbTaskRunner.setStep(this.step);
        this.internalDbTaskRunner.setStatus(status);
    }

    public void setAllDone() {
        globalstep = ALLDONETASK;
        step = 0;
        status = TaskStatus.OK;
        this.internalDbTaskRunner.setGlobalstep(globalstep);
        this.internalDbTaskRunner.setStep(this.step);
        this.internalDbTaskRunner.setStatus(status);
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
        AbstractTask task = TaskType.getTaskFromId(name, arg, session);
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
                this.internalDbTaskRunner.setStep(this.step);
                this.internalDbTaskRunner.setStatus(status);
                return;
            } catch (OpenR66RunnerErrorException e) {
                status = TaskStatus.ERROR;
                this.internalDbTaskRunner.setStep(this.step);
                this.internalDbTaskRunner.setStatus(status);
                throw new OpenR66RunnerErrorException("Runner is in error: " +
                        e.getMessage(), e);
            }
            if (future.isCancelled()) {
                status = TaskStatus.ERROR;
                this.internalDbTaskRunner.setStep(this.step);
                this.internalDbTaskRunner.setStatus(status);
                throw new OpenR66RunnerErrorException("Runner is error: " +
                        future.getCause().getMessage(), future.getCause());
            }
            step ++;
        }
    }

    public boolean ready() {
        return globalstep > PRETASK;
    }

    public boolean isFinished() {
        return (globalstep == ALLDONETASK) || (globalstep == ERRORTASK && status == TaskStatus.OK);
    }
    /**
     * @return the rank
     */
    public int getRank() {
        return rank;
    }
    /**
     * To set the rank at startup of the request if the request specify a specific rank
     * @param rank
     */
    public void setRankAtStartup(int rank) {
        this.rank = rank;
        this.internalDbTaskRunner.setRank(this.rank);
    }
    /**
     * Increment the rank
     */
    public void incrementRank() {
        rank ++;
        this.internalDbTaskRunner.setRank(this.rank);
        if ((rank % 10) == 0) {
            // Save each 10 blocks
            try {
                this.internalDbTaskRunner.update();
            } catch (OpenR66DatabaseException e) {
                logger.warn("Cannot update Runner", e);
            }
        }
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
        this.internalDbTaskRunner.setFileMoved(this.isFileMoved);
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
        this.internalDbTaskRunner.setOriginalFilename(this.originalFilename);
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
     * @throws OpenR66RunnerErrorException
     */
    public void saveStatus() throws OpenR66RunnerErrorException {
        logger.info(GgInternalLogger.getRankMethodAndLine(3) + " " +
                toString());
        try {
            this.internalDbTaskRunner.update();
        } catch (OpenR66DatabaseException e) {
            throw new OpenR66RunnerErrorException(e);
        }
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
