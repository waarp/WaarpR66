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
package openr66.database.data;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;

import openr66.context.R66Rule;
import openr66.context.R66Session;
import openr66.context.task.AbstractTask;
import openr66.context.task.TaskType;
import openr66.context.task.exception.OpenR66RunnerEndTasksException;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.DbConstant;
import openr66.database.DbPreparedStatement;
import openr66.database.DbSession;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseNoDataException;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.database.model.DbModelFactory;
import openr66.protocol.config.Configuration;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.utils.R66Future;

/**
 * @author Frederic Bregier
 *
 */
public class DbTaskRunner extends AbstractDbData {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(DbTaskRunner.class);

    public static enum Columns {
        GLOBALSTEP, STEP, RANK, STEPSTATUS, RETRIEVEMODE, FILENAME, ISMOVED, IDRULE,
        BLOCKSIZE, ORIGINALNAME, FILEINFO, MODE, REQUESTER, REQUESTED,
        START, STOP,
        UPDATEDINFO,
        SPECIALID;
    }
    public static int [] dbTypes = {
        Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.BIT,
        Types.VARCHAR, Types.BIT, Types.VARCHAR,
        Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.VARCHAR,
        Types.TIMESTAMP, Types.TIMESTAMP,
        Types.INTEGER, Types.VARCHAR
    };
    public static String table = " RUNNER ";
    public static String fieldseq = "RUNSEQ";

    /**
     * GlobalStep Value
     */
    public static final int NOTASK = -1;
    /**
     * GlobalStep Value
     */
    public static final int PRETASK = 0;
    /**
     * GlobalStep Value
     */
    public static final int POSTTASK = 1;
    /**
     * GlobalStep Value
     */
    public static final int ERRORTASK = 2;
    /**
     * GlobalStep Value
     */
    public static final int TRANSFERTASK = 3;
    /**
     * GlobalStep Value
     */
    public static final int ALLDONETASK = 4;
    /**
     * GlobalStep Status
     * @author Frederic Bregier
     *
     */
    public static enum TaskStatus {
        UNKNOWN, RUNNING, OK, WARNING, ERROR;
    }

    // Values
    private final R66Rule rule;
    private final R66Session session;
    private int globalstep = NOTASK;
    private int step = NOTASK;
    private int rank = 0;
    private TaskStatus status = TaskStatus.UNKNOWN;
    private long specialId;
    private boolean isRetrieve;
    private String filename;
    private boolean isFileMoved = false;
    private String ruleId;
    private int blocksize;
    private String originalFilename;
    private String fileInformation;
    private int mode;
    private String requesterHostId;
    private String requestedHostId;

    private Timestamp start;
    private Timestamp stop;

    private int updatedInfo;
    private boolean isSaved = false;

    // ALL TABLE SHOULD IMPLEMENT THIS
    private DbValue primaryKey = new DbValue(specialId, Columns.SPECIALID.name());
    private DbValue[] otherFields = {
      // GLOBALSTEP, STEP, RANK, STEPSTATUS, RETRIEVEMODE, FILENAME, ISMOVED, IDRULE,
      // BLOCKSIZE, ORIGINALNAME, FILEINFO, MODE, REQUESTER, REQUESTED
      // START, STOP
      // UPDATEDINFO
      new DbValue(globalstep, Columns.GLOBALSTEP.name()),
      new DbValue(step, Columns.STEP.name()),
      new DbValue(rank, Columns.RANK.name()),
      new DbValue(status.ordinal(), Columns.STEPSTATUS.name()),
      new DbValue(isRetrieve, Columns.RETRIEVEMODE.name()),
      new DbValue(filename, Columns.FILENAME.name()),
      new DbValue(isFileMoved, Columns.ISMOVED.name()),
      new DbValue(ruleId, Columns.IDRULE.name()),
      new DbValue(blocksize, Columns.BLOCKSIZE.name()),
      new DbValue(originalFilename, Columns.ORIGINALNAME.name()),
      new DbValue(fileInformation, Columns.FILEINFO.name()),
      new DbValue(mode, Columns.MODE.name()),
      new DbValue(requesterHostId, Columns.REQUESTER.name()),
      new DbValue(requestedHostId, Columns.REQUESTED.name()),
      new DbValue(start, Columns.START.name()),
      new DbValue(stop, Columns.STOP.name()),
      new DbValue(updatedInfo, Columns.UPDATEDINFO.name())
    };
    private DbValue[] allFields = {
      otherFields[0], otherFields[1], otherFields[2], otherFields[3],
      otherFields[4], otherFields[5], otherFields[6], otherFields[7], otherFields[8],
      otherFields[9], otherFields[10], otherFields[11], otherFields[12], otherFields[13],
      otherFields[14], otherFields[15], otherFields[16],
      primaryKey
    };
    private static final String selectAllFields =
        Columns.GLOBALSTEP.name()+","+Columns.STEP.name()+
        ","+Columns.RANK.name()+","+Columns.STEPSTATUS.name()+","+Columns.RETRIEVEMODE.name()+
        ","+Columns.FILENAME.name()+","+Columns.ISMOVED.name()+","+Columns.IDRULE.name()+
        ","+Columns.BLOCKSIZE.name()+","+Columns.ORIGINALNAME.name()+","+Columns.FILEINFO.name()+
        ","+Columns.MODE.name()+","+Columns.REQUESTER.name()+","+Columns.REQUESTED.name()+
        ","+Columns.START.name()+","+Columns.STOP.name()+
        ","+Columns.UPDATEDINFO.name()+
        ","+Columns.SPECIALID.name();
    private static final String updateAllFields =
        Columns.GLOBALSTEP.name()+"=?,"+Columns.STEP.name()+
        "=?,"+Columns.RANK.name()+"=?,"+Columns.STEPSTATUS.name()+"=?,"+Columns.RETRIEVEMODE.name()+
        "=?,"+Columns.FILENAME.name()+"=?,"+Columns.ISMOVED.name()+"=?,"+Columns.IDRULE.name()+
        "=?,"+Columns.BLOCKSIZE.name()+"=?,"+Columns.ORIGINALNAME.name()+"=?,"+Columns.FILEINFO.name()+
        "=?,"+Columns.MODE.name()+"=?,"+Columns.REQUESTER.name()+"=?,"+Columns.REQUESTED.name()+
        "=?,"+Columns.START.name()+"=?,"+Columns.STOP.name()+"=?,"+Columns.UPDATEDINFO.name()+"=?";
    private static final String insertAllValues = " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";


    public DbTaskRunner(R66Session session, R66Rule rule, boolean isRetrieve,
            RequestPacket requestPacket) throws OpenR66DatabaseException {
        this.session = session;
        this.rule = rule;
        this.ruleId = this.rule.idRule;
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

        this.start = new Timestamp(System.currentTimeMillis());
        this.updatedInfo = UNKNOWN;
        this.setToArray();
        this.isSaved = false;
        specialId = requestPacket.getSpecialId();
        this.insert();
    }

    public DbTaskRunner(R66Session session, R66Rule rule, long id) throws OpenR66DatabaseException {
        this.session = session;
        this.rule = rule;
        specialId = id;

        this.select();
        if (! this.ruleId.equals(rule.idRule)) {
            throw new OpenR66DatabaseNoDataException("Rule does not correspond");
        }
    }

    @Override
    protected void setToArray() {
        allFields[Columns.SPECIALID.ordinal()].setValue(this.specialId);
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(this.globalstep);
        allFields[Columns.STEP.ordinal()].setValue(this.step);
        allFields[Columns.RANK.ordinal()].setValue(this.rank);
        allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.ordinal());
        allFields[Columns.RETRIEVEMODE.ordinal()].setValue(this.isRetrieve);
        allFields[Columns.FILENAME.ordinal()].setValue(this.filename);
        allFields[Columns.ISMOVED.ordinal()].setValue(this.isFileMoved);
        allFields[Columns.IDRULE.ordinal()].setValue(this.ruleId);
        allFields[Columns.BLOCKSIZE.ordinal()].setValue(this.blocksize);
        allFields[Columns.ORIGINALNAME.ordinal()].setValue(this.originalFilename);
        allFields[Columns.FILEINFO.ordinal()].setValue(this.fileInformation);
        allFields[Columns.MODE.ordinal()].setValue(this.mode);
        allFields[Columns.REQUESTER.ordinal()].setValue(this.requesterHostId);
        allFields[Columns.REQUESTED.ordinal()].setValue(this.requestedHostId);
        allFields[Columns.START.ordinal()].setValue(this.start);
        this.stop = new Timestamp(System.currentTimeMillis());
        allFields[Columns.STOP.ordinal()].setValue(this.stop);
        allFields[Columns.UPDATEDINFO.ordinal()].setValue(this.updatedInfo);
    }
    @Override
    protected void setFromArray() throws OpenR66DatabaseSqlError {
        this.specialId = (Long) allFields[Columns.SPECIALID.ordinal()].getValue();
        this.globalstep = (Integer) allFields[Columns.GLOBALSTEP.ordinal()].getValue();
        this.step = (Integer) allFields[Columns.STEP.ordinal()].getValue();
        this.rank = (Integer) allFields[Columns.RANK.ordinal()].getValue();
        this.status = TaskStatus.values()[((Integer) allFields[Columns.STEPSTATUS.ordinal()].getValue())];
        this.isRetrieve = (Boolean) allFields[Columns.RETRIEVEMODE.ordinal()].getValue();
        this.filename = (String) allFields[Columns.FILENAME.ordinal()].getValue();
        this.isFileMoved = (Boolean) allFields[Columns.ISMOVED.ordinal()].getValue();
        this.ruleId = (String) allFields[Columns.IDRULE.ordinal()].getValue();
        this.blocksize = (Integer) allFields[Columns.BLOCKSIZE.ordinal()].getValue();
        this.originalFilename = (String) allFields[Columns.ORIGINALNAME.ordinal()].getValue();
        this.fileInformation = (String) allFields[Columns.FILEINFO.ordinal()].getValue();
        this.mode = (Integer) allFields[Columns.MODE.ordinal()].getValue();
        this.requesterHostId = (String) allFields[Columns.REQUESTER.ordinal()].getValue();
        this.requestedHostId = (String) allFields[Columns.REQUESTED.ordinal()].getValue();
        this.start = (Timestamp) allFields[Columns.START.ordinal()].getValue();
        this.stop = (Timestamp) allFields[Columns.STOP.ordinal()].getValue();
        this.updatedInfo = (Integer) allFields[Columns.UPDATEDINFO.ordinal()].getValue();
    }
    /**
     * Empty private constructor
     */
    private DbTaskRunner() {
        session = null;
        rule = null;
    }
    /* (non-Javadoc)
     * @see openr66.database.data.AbstractDbData#delete()
     */
    @Override
    public void delete() throws OpenR66DatabaseException {
        DbPreparedStatement preparedStatement =
            new DbPreparedStatement(DbConstant.admin.session);
        try {
            preparedStatement.createPrepareStatement("DELETE FROM "+
                    table+" WHERE "+Columns.SPECIALID.name()+" = ?");
            primaryKey.setValue(specialId);
            this.setValue(preparedStatement, primaryKey);
            int count = preparedStatement.executeUpdate();
            preparedStatement.realClose();
            if (count <= 0) {
                throw new OpenR66DatabaseNoDataException("No row found");
            }
            this.isSaved = false;
        } finally {
            preparedStatement.realClose();
        }
    }

    /* (non-Javadoc)
     * @see openr66.database.data.AbstractDbData#insert()
     */
    @Override
    public void insert() throws OpenR66DatabaseException {
        if (this.isSaved) {
            return;
        }
        // First need to find a new id if id is not ok
        if (this.specialId == DbConstant.ILLEGALVALUE) {
            this.specialId = DbModelFactory.dbModel.nextSequence();
            primaryKey.setValue(this.specialId);
        }
        this.setToArray();
        DbPreparedStatement preparedStatement =
            new DbPreparedStatement(DbConstant.admin.session);
        try {
            preparedStatement.createPrepareStatement("INSERT INTO "+table+
                    " ("+selectAllFields+") VALUES "+
                    insertAllValues);
            this.setValues(preparedStatement, allFields);
            int count = preparedStatement.executeUpdate();
            preparedStatement.realClose();
            if (count <= 0) {
                throw new OpenR66DatabaseNoDataException("No row found");
            }
            this.isSaved = true;
        } finally {
            preparedStatement.realClose();
        }
    }

    /* (non-Javadoc)
     * @see openr66.database.data.AbstractDbData#select()
     */
    @Override
    public void select() throws OpenR66DatabaseException {
        DbPreparedStatement preparedStatement =
            new DbPreparedStatement(DbConstant.admin.session);
        preparedStatement.createPrepareStatement("SELECT "+selectAllFields+" FROM "+
                table+" WHERE "+Columns.SPECIALID.name()+" = ?");
        try {
            primaryKey.setValue(specialId);
            this.setValue(preparedStatement, primaryKey);
            preparedStatement.executeQuery();
            if (preparedStatement.getNext()) {
                this.getValues(preparedStatement, allFields);
                this.setFromArray();
                this.isSaved = true;
            } else {
                throw new OpenR66DatabaseNoDataException("No row found");
            }
        } finally {
            preparedStatement.realClose();
        }
    }

    /* (non-Javadoc)
     * @see openr66.database.data.AbstractDbData#update()
     */
    @Override
    public void update() throws OpenR66DatabaseException {
        if (this.isSaved) {
            return;
        }
        this.setToArray();
        DbPreparedStatement preparedStatement =
            new DbPreparedStatement(DbConstant.admin.session);
        try {
            preparedStatement.createPrepareStatement("UPDATE "+table+" SET "+updateAllFields+
                    " WHERE "+Columns.SPECIALID.name()+" = ?");
            this.setValues(preparedStatement, allFields);
            int count = preparedStatement.executeUpdate();
            preparedStatement.realClose();
            if (count <= 0) {
                throw new OpenR66DatabaseNoDataException("No row found");
            }
            this.isSaved = true;
        } finally {
            preparedStatement.realClose();
        }
    }

    /* (non-Javadoc)
     * @see openr66.database.data.AbstractDbData#changeUpdatedInfo(int)
     */
    @Override
    public void changeUpdatedInfo(int status) {
        if (this.updatedInfo != status) {
            this.updatedInfo = status;
            allFields[Columns.UPDATEDINFO.ordinal()].setValue(this.updatedInfo);
            this.isSaved = false;
        }
    }

    /**
     * To set the rank at startup of the request if the request specify a specific rank
     * @param RANK the RANK to set
     */
    public void setRankAtStartup(int rank) {
        if (this.rank != rank) {
            this.rank = rank;
            allFields[Columns.RANK.ordinal()].setValue(this.rank);
            this.isSaved = false;
        }
    }

    /**
     * @param FILENAME the FILENAME to set
     */
    public void setFilename(String filename) {
        if (this.filename != filename) {
            this.filename = filename;
            allFields[Columns.FILENAME.ordinal()].setValue(this.filename);
            this.isSaved = false;
        }
    }

    /**
     * @param isFileMoved the isFileMoved to set
     */
    public void setFileMoved(boolean isFileMoved) {
        if (this.isFileMoved != isFileMoved) {
            this.isFileMoved = isFileMoved;
            allFields[Columns.ISMOVED.ordinal()].setValue(this.isFileMoved);
            this.isSaved = false;
        }
    }

    /**
     * @param originalFilename the originalFilename to set
     */
    public void setOriginalFilename(String originalFilename) {
        if (this.originalFilename != originalFilename) {
            this.originalFilename = originalFilename;
            allFields[Columns.ORIGINALNAME.ordinal()].setValue(this.originalFilename);
            this.isSaved = false;
        }
    }

    public RequestPacket getRequest() {
        // FIXME
        return new RequestPacket(this.ruleId,this.mode,this.originalFilename,this.blocksize,this.rank,
                this.specialId,this.fileInformation);
    }

    /**
     * @return the rank
     */
    public int getRank() {
        return rank;
    }
    /**
     * Change the status from Task Execution
     * @param status
     */
    public void setExecutionStatus(TaskStatus status) {
        this.status = status;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.ordinal());
        this.isSaved = false;
    }
    /**
     * @return the status
     */
    public TaskStatus getStatus() {
        return status;
    }

    /**
     * @return the isRetrieve
     */
    public boolean isRetrieve() {
        return isRetrieve;
    }

    /**
     * @return the isFileMoved
     */
    public boolean isFileMoved() {
        return isFileMoved;
    }

    /**
     * @return the blocksize
     */
    public int getBlocksize() {
        return blocksize;
    }

    /**
     * @return the originalFilename
     */
    public String getOriginalFilename() {
        return originalFilename;
    }

    /**
     * @return the fileInformation
     */
    public String getFileInformation() {
        return fileInformation;
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

    public boolean ready() {
        return globalstep > PRETASK;
    }

    public boolean isFinished() {
        return (globalstep == ALLDONETASK) || (globalstep == ERRORTASK && status == TaskStatus.OK);
    }

    public void setPreTask(int step) {
        globalstep = PRETASK;
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(this.globalstep);
        this.step = step;
        allFields[Columns.STEP.ordinal()].setValue(this.step);
        status = TaskStatus.RUNNING;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.ordinal());
        this.isSaved = false;
    }

    public void setTransferTask(int rank) {
        globalstep = TRANSFERTASK;
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(this.globalstep);
        this.rank = rank;
        allFields[Columns.RANK.ordinal()].setValue(this.rank);
        status = TaskStatus.RUNNING;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.ordinal());
        this.isSaved = false;
    }

    public int finishTransferTask(boolean status) {
        if (status) {
            this.status = TaskStatus.OK;
        } else {
            this.status = TaskStatus.ERROR;
        }
        allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.ordinal());
        this.isSaved = false;
        return rank;
    }

    public void setPostTask(int step) {
        globalstep = POSTTASK;
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(this.globalstep);
        this.step = step;
        allFields[Columns.STEP.ordinal()].setValue(this.step);
        status = TaskStatus.RUNNING;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.ordinal());
        this.isSaved = false;
    }

    public void setErrorTask(int step) {
        globalstep = ERRORTASK;
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(this.globalstep);
        this.step = step;
        allFields[Columns.STEP.ordinal()].setValue(this.step);
        status = TaskStatus.RUNNING;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.ordinal());
        this.isSaved = false;
    }

    public void setAllDone() {
        globalstep = ALLDONETASK;
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(this.globalstep);
        step = 0;
        allFields[Columns.STEP.ordinal()].setValue(this.step);
        //FIXME set by POST status = TaskStatus.OK;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.ordinal());
        this.isSaved = false;
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
        int delay = Integer.parseInt(tasks[step][2]);
        AbstractTask task = TaskType.getTaskFromId(name, arg, delay, session);
        task.run();
        task.getFutureCompletion().awaitUninterruptibly();
        return task.getFutureCompletion();
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
                if (status == TaskStatus.RUNNING) {
                    status = TaskStatus.OK;
                }
                allFields[Columns.STEP.ordinal()].setValue(this.step);
                allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.ordinal());
                this.isSaved = false;
                return;
            } catch (OpenR66RunnerErrorException e) {
                status = TaskStatus.ERROR;
                allFields[Columns.STEP.ordinal()].setValue(this.step);
                allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.ordinal());
                this.isSaved = false;
                throw new OpenR66RunnerErrorException("Runner is in error: " +
                        e.getMessage(), e);
            }
            if (future.isCancelled()) {
                status = TaskStatus.ERROR;
                allFields[Columns.STEP.ordinal()].setValue(this.step);
                allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.ordinal());
                this.isSaved = false;
                throw new OpenR66RunnerErrorException("Runner is error: " +
                        future.getCause().getMessage(), future.getCause());
            }
            step ++;
        }
    }
    /**
     * Increment the rank
     */
    public void incrementRank() {
        rank ++;
        allFields[Columns.RANK.ordinal()].setValue(this.rank);
        this.isSaved = false;
        if ((rank % 10) == 0) {
            // Save each 10 blocks
            try {
                this.update();
            } catch (OpenR66DatabaseException e) {
                logger.warn("Cannot update Runner", e);
            }
        }
    }
    /**
     * This method is to be called each time an operation is happening on Runner
     * @throws OpenR66RunnerErrorException
     */
    public void saveStatus() throws OpenR66RunnerErrorException {
        logger.info(GgInternalLogger.getRankMethodAndLine(3) + " " +
                toString());
        try {
            this.update();
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
    /**
     *
     * @param status the status to match or UNKNOWN for all
     * @return All index (specialId) that match the status
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     * @throws OpenR66DatabaseNoDataException
     */
    public static ArrayList<Integer> getAllRunner(int status) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError, OpenR66DatabaseNoDataException {
        DbPreparedStatement preparedStatement =
            new DbPreparedStatement(DbConstant.admin.session);
        DbTaskRunner runner = null;
        runner = new DbTaskRunner();
        try {
            if (status == UNKNOWN) {
                preparedStatement.createPrepareStatement("SELECT COUNT("+runner.primaryKey+") FROM "+
                        table);
            } else {
                preparedStatement.createPrepareStatement("SELECT COUNT("+runner.primaryKey+") FROM "+
                        table+" WHERE "+Columns.UPDATEDINFO.name()+" = ?");
                runner.allFields[Columns.UPDATEDINFO.ordinal()].setValue(status);
                runner.setValue(preparedStatement, runner.allFields[Columns.UPDATEDINFO.ordinal()]);
            }
            preparedStatement.executeQuery();
            int count = 0;
            if (preparedStatement.getNext()) {
                ResultSet rs = preparedStatement.getResultSet();
                try {
                    count = rs.getInt(1);
                } catch (SQLException e) {
                    DbSession.error(e);
                    throw new OpenR66DatabaseSqlError("Getting values in error: Integer for Count", e);
                }
            } else {
                throw new OpenR66DatabaseNoDataException("No row found");
            }
            preparedStatement.realClose();
            ArrayList<Integer> result = new ArrayList<Integer>(count);
            if (status == UNKNOWN) {
                preparedStatement.createPrepareStatement("SELECT "+runner.primaryKey+" FROM "+
                        table);
            } else {
                preparedStatement.createPrepareStatement("SELECT "+runner.primaryKey+" FROM "+
                        table+" WHERE "+Columns.UPDATEDINFO.name()+" = ?");
                runner.allFields[Columns.UPDATEDINFO.ordinal()].setValue(status);
                runner.setValue(preparedStatement, runner.allFields[Columns.UPDATEDINFO.ordinal()]);
            }
            preparedStatement.executeQuery();
            while (preparedStatement.getNext()) {
                runner.getValue(preparedStatement, runner.primaryKey);
                Integer newval = new Integer((Integer) runner.primaryKey.value);
                result.add(newval);
            }
            return result;
        } finally {
            preparedStatement.realClose();
        }
    }
}
