/**
 * Copyright 2009, Frederic Bregier, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package openr66.task;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.filesystem.R66Rule;
import openr66.filesystem.R66Session;
import openr66.protocol.exception.OpenR66RunnerEndTasksException;
import openr66.protocol.exception.OpenR66RunnerErrorException;
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

    private static final int NOTASK = -1;
    private static final int PRETASK = 0;
    private static final int POSTTASK = 1;
    private static final int ERRORTASK = 2;
    private static final int TRANSFERTASK = 3;
    private static final int ALLDONETASK = 4;

    public static enum TaskStatus {
        UNKNOWN,
        RUNNING,
        OK,
        ERROR;
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

    public TaskRunner(R66Session session, R66Rule rule, boolean isRetrieve) {
        this.session = session;
        this.rule = rule;
        this.status = TaskStatus.UNKNOWN;
        long newId = this.session.getLocalChannelReference().getRemoteId();
        newId = newId << 32;
        //FIXME need a way to check if it does not already exist
        this.specialId = newId + this.session.getLocalChannelReference().getLocalId();
        this.isRetrieve = isRetrieve;
    }

    public TaskRunner(R66Session session, R66Rule rule, long id) {
        this.session = session;
        this.rule = rule;
        this.status = TaskStatus.UNKNOWN;
        this.specialId = id;
        // FIXME load from database
        this.isRetrieve = false;// XXX FIXME TODO WARNING FALSE!!!
    }
    /**
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * @param filename the filename to set
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
        return this.isRetrieve;
    }
    public void setPreTask(int step) {
        this.globalstep = PRETASK;
        this.step = step;
        this.status = TaskStatus.RUNNING;
    }
    public void setTransferTask(int rank) {
        this.globalstep = TRANSFERTASK;
        this.rank = rank;
        this.status = TaskStatus.RUNNING;
    }
    public int finishTransferTask(boolean status) {
        if (status) {
            this.status = TaskStatus.OK;
        } else {
            this.status = TaskStatus.ERROR;
        }
        return this.rank;
    }
    public void setPostTask(int step) {
        this.globalstep = POSTTASK;
        this.step = step;
        this.status = TaskStatus.RUNNING;
    }
    public void setErrorTask(int step) {
        this.globalstep = ERRORTASK;
        this.step = step;
        this.status = TaskStatus.RUNNING;
    }
    public void setAllDone() {
        this.globalstep = ALLDONETASK;
        this.step = 0;
        this.status = TaskStatus.OK;
    }

    private R66Future runNextTask(String [][] tasks) throws OpenR66RunnerEndTasksException, OpenR66RunnerErrorException {
        if (tasks.length <= this.step) {
            throw new OpenR66RunnerEndTasksException();
        }
        String name = tasks[this.step][0];
        String arg = tasks[this.step][1];
        AbstractTask task = TaskFactory.getTaskFromId(name, arg, session);
        task.run();
        task.futureCompletion.awaitUninterruptibly();
        return task.futureCompletion;
    }
    private R66Future runNext() throws OpenR66RunnerEndTasksException, OpenR66RunnerErrorException {
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
        while (true) {
            try {
                future = runNext();
            } catch (OpenR66RunnerEndTasksException e) {
                this.status = TaskStatus.OK;
                return;
            }
            if (future.isCancelled()) {
                this.status = TaskStatus.ERROR;
                throw new OpenR66RunnerErrorException("Runner is error: "+future.getCause().getMessage(),
                        future.getCause());
            }
            this.step++;
        }
    }
    public boolean ready() {
        return (globalstep != NOTASK);
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
        rank++;
    }

    /**
     * @return the isFileMoved
     */
    public boolean isFileMoved() {
        return isFileMoved;
    }

    /**
     * @param isFileMoved the isFileMoved to set
     */
    public void setFileMoved(boolean isFileMoved) {
        this.isFileMoved = isFileMoved;
    }

    /**
     * This method is to be called each time an operation is happening on Runner
     */
    public void saveStatus() {
        // FIXME should save status to DB
        // FIXME need a specialID that could be reused over time
        // save: rulename, globalstep, setp, rank, status, specialId, filename, isRetrieve
        logger.info(GgInternalLogger.getRankMethodAndLine(3)+" "+this.toString());
    }
    public void clear() {

    }
    public String toString() {
        return "Run: "+(rule != null ? rule.toString() : "no Rule")+" on "+filename+" step: "+globalstep+":"+step+":"+status+":"+rank+" SpecialId: "+specialId+" isRetr: "+isRetrieve+" isMoved: "+isFileMoved;
    }
}
