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
package openr66.database.data;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;

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
 * Task Runner from pre operation to transfer to post operation, except in case of error
 *
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
        GLOBALSTEP,
        GLOBALLASTSTEP,
        STEP,
        RANK,
        STEPSTATUS,
        RETRIEVEMODE,
        FILENAME,
        ISMOVED,
        IDRULE,
        BLOCKSIZE,
        ORIGINALNAME,
        FILEINFO,
        MODE,
        REQUESTER,
        START,
        STOP,
        UPDATEDINFO,
        REQUESTED,
        SPECIALID;
    }

    public static int[] dbTypes = {
            Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER,
            Types.INTEGER, Types.BIT, Types.VARCHAR, Types.BIT, Types.VARCHAR,
            Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
            Types.VARCHAR, Types.TIMESTAMP, Types.TIMESTAMP, Types.INTEGER,
            Types.VARCHAR, Types.BIGINT };

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
     *
     * @author Frederic Bregier
     *
     */
    public static enum TaskStatus {
        UNKNOWN, RUNNING, OK, WARNING, ERROR;
    }

    // Values
    private final DbR66Rule rule;

    private final R66Session session;

    private int globalstep = NOTASK;

    private int globallaststep = NOTASK;

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
    private final DbValue primaryKey[] = {
            new DbValue(requestedHostId, Columns.REQUESTED.name()),
            new DbValue(specialId, Columns.SPECIALID.name()) };

    private final DbValue[] otherFields = {
            // GLOBALSTEP, GLOBALLASTSTEP, STEP, RANK, STEPSTATUS, RETRIEVEMODE,
            // FILENAME, ISMOVED, IDRULE,
            // BLOCKSIZE, ORIGINALNAME, FILEINFO, MODE, REQUESTER, REQUESTED
            // START, STOP
            // UPDATEDINFO
            new DbValue(globalstep, Columns.GLOBALSTEP.name()),
            new DbValue(globallaststep, Columns.GLOBALLASTSTEP.name()),
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
            new DbValue(start, Columns.START.name()),
            new DbValue(stop, Columns.STOP.name()),
            new DbValue(updatedInfo, Columns.UPDATEDINFO.name()) };

    private final DbValue[] allFields = {
            otherFields[0], otherFields[1], otherFields[2], otherFields[3],
            otherFields[4], otherFields[5], otherFields[6], otherFields[7],
            otherFields[8], otherFields[9], otherFields[10], otherFields[11],
            otherFields[12], otherFields[13], otherFields[14], otherFields[15],
            otherFields[16], primaryKey[0], primaryKey[1] };

    private static final String selectAllFields = Columns.GLOBALSTEP.name() +
            "," + Columns.GLOBALLASTSTEP.name() + "," + Columns.STEP.name() +
            "," + Columns.RANK.name() + "," + Columns.STEPSTATUS.name() + "," +
            Columns.RETRIEVEMODE.name() + "," + Columns.FILENAME.name() + "," +
            Columns.ISMOVED.name() + "," + Columns.IDRULE.name() + "," +
            Columns.BLOCKSIZE.name() + "," + Columns.ORIGINALNAME.name() + "," +
            Columns.FILEINFO.name() + "," + Columns.MODE.name() + "," +
            Columns.REQUESTER.name() + "," + Columns.START.name() + "," +
            Columns.STOP.name() + "," + Columns.UPDATEDINFO.name() + "," +
            Columns.REQUESTED.name() + "," + Columns.SPECIALID.name();

    private static final String updateAllFields = Columns.GLOBALSTEP.name() +
            "=?," + Columns.GLOBALLASTSTEP.name() + "=?," +
            Columns.STEP.name() + "=?," + Columns.RANK.name() + "=?," +
            Columns.STEPSTATUS.name() + "=?," + Columns.RETRIEVEMODE.name() +
            "=?," + Columns.FILENAME.name() + "=?," + Columns.ISMOVED.name() +
            "=?," + Columns.IDRULE.name() + "=?," + Columns.BLOCKSIZE.name() +
            "=?," + Columns.ORIGINALNAME.name() + "=?," +
            Columns.FILEINFO.name() + "=?," + Columns.MODE.name() + "=?," +
            Columns.REQUESTER.name() + "=?," + Columns.START.name() + "=?," +
            Columns.STOP.name() + "=?," + Columns.UPDATEDINFO.name() + "=?";

    private static final String insertAllValues = " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";

    /**
     *
     * @param session
     * @param requestPacket
     * @return The associated requested Host Id
     */
    public static String getRequested(R66Session session,
            RequestPacket requestPacket) {
        if (requestPacket.isToValidate()) {
            // the request is initiated and sent by the requester
            return Configuration.configuration.HOST_ID;
        } else {
            // the request is sent after acknowledge by the requested
            return session.getAuth().getUser();
        }
    }

    /**
     *
     * @param session
     * @param requestPacket
     * @return The associated requester Host Id
     */
    public static String getRequester(R66Session session,
            RequestPacket requestPacket) {
        if (requestPacket.isToValidate()) {
            return session.getAuth().getUser();
        } else {
            return Configuration.configuration.HOST_ID;
        }
    }
    /**
     *
     * @param session
     * @param rule
     * @param isRetrieve
     * @param requestPacket
     * @throws OpenR66DatabaseException
     */
    public DbTaskRunner(R66Session session, DbR66Rule rule, boolean isRetrieve,
            RequestPacket requestPacket) throws OpenR66DatabaseException {
        this.session = session;
        this.rule = rule;
        ruleId = this.rule.idRule;
        rank = requestPacket.getRank();
        status = TaskStatus.UNKNOWN;
        this.isRetrieve = isRetrieve;
        filename = requestPacket.getFilename();
        blocksize = requestPacket.getBlocksize();
        originalFilename = requestPacket.getFilename();
        fileInformation = requestPacket.getFileInformation();
        mode = requestPacket.getMode();
        requesterHostId = getRequester(session, requestPacket);
        requestedHostId = getRequested(session, requestPacket);

        start = new Timestamp(System.currentTimeMillis());
        updatedInfo = UpdatedInfo.UNKNOWN.ordinal();
        setToArray();
        isSaved = false;
        specialId = requestPacket.getSpecialId();
        insert();
    }
    /**
     *
     * @param session
     * @param rule
     * @param id
     * @param requested
     * @throws OpenR66DatabaseException
     */
    public DbTaskRunner(R66Session session, DbR66Rule rule, long id,
            String requested) throws OpenR66DatabaseException {
        this.session = session;
        this.rule = rule;

        specialId = id;
        // retrieving a task should be made from the requester, but the caller
        // is responsible of this
        requestedHostId = requested;

        select();
        if (!ruleId.equals(rule.idRule)) {
            throw new OpenR66DatabaseNoDataException("Rule does not correspond");
        }
    }

    @Override
    protected void setToArray() {
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(globalstep);
        allFields[Columns.GLOBALLASTSTEP.ordinal()].setValue(globallaststep);
        allFields[Columns.STEP.ordinal()].setValue(step);
        allFields[Columns.RANK.ordinal()].setValue(rank);
        allFields[Columns.STEPSTATUS.ordinal()].setValue(status.ordinal());
        allFields[Columns.RETRIEVEMODE.ordinal()].setValue(isRetrieve);
        allFields[Columns.FILENAME.ordinal()].setValue(filename);
        allFields[Columns.ISMOVED.ordinal()].setValue(isFileMoved);
        allFields[Columns.IDRULE.ordinal()].setValue(ruleId);
        allFields[Columns.BLOCKSIZE.ordinal()].setValue(blocksize);
        allFields[Columns.ORIGINALNAME.ordinal()].setValue(originalFilename);
        allFields[Columns.FILEINFO.ordinal()].setValue(fileInformation);
        allFields[Columns.MODE.ordinal()].setValue(mode);
        allFields[Columns.REQUESTER.ordinal()].setValue(requesterHostId);
        allFields[Columns.START.ordinal()].setValue(start);
        stop = new Timestamp(System.currentTimeMillis());
        allFields[Columns.STOP.ordinal()].setValue(stop);
        allFields[Columns.UPDATEDINFO.ordinal()].setValue(updatedInfo);
        allFields[Columns.REQUESTED.ordinal()].setValue(requestedHostId);
        allFields[Columns.SPECIALID.ordinal()].setValue(specialId);
    }

    @Override
    protected void setFromArray() throws OpenR66DatabaseSqlError {
        globalstep = (Integer) allFields[Columns.GLOBALSTEP.ordinal()]
                .getValue();
        globallaststep = (Integer) allFields[Columns.GLOBALLASTSTEP.ordinal()]
                .getValue();
        step = (Integer) allFields[Columns.STEP.ordinal()].getValue();
        rank = (Integer) allFields[Columns.RANK.ordinal()].getValue();
        status = TaskStatus.values()[((Integer) allFields[Columns.STEPSTATUS
                .ordinal()].getValue())];
        isRetrieve = (Boolean) allFields[Columns.RETRIEVEMODE.ordinal()]
                .getValue();
        filename = (String) allFields[Columns.FILENAME.ordinal()].getValue();
        isFileMoved = (Boolean) allFields[Columns.ISMOVED.ordinal()].getValue();
        ruleId = (String) allFields[Columns.IDRULE.ordinal()].getValue();
        blocksize = (Integer) allFields[Columns.BLOCKSIZE.ordinal()].getValue();
        originalFilename = (String) allFields[Columns.ORIGINALNAME.ordinal()]
                .getValue();
        fileInformation = (String) allFields[Columns.FILEINFO.ordinal()]
                .getValue();
        mode = (Integer) allFields[Columns.MODE.ordinal()].getValue();
        requesterHostId = (String) allFields[Columns.REQUESTER.ordinal()]
                .getValue();
        start = (Timestamp) allFields[Columns.START.ordinal()].getValue();
        stop = (Timestamp) allFields[Columns.STOP.ordinal()].getValue();
        updatedInfo = (Integer) allFields[Columns.UPDATEDINFO.ordinal()]
                .getValue();
        requestedHostId = (String) allFields[Columns.REQUESTED.ordinal()]
                .getValue();
        specialId = (Long) allFields[Columns.SPECIALID.ordinal()].getValue();
    }

    /**
     * Empty private constructor
     */
    private DbTaskRunner() {
        session = null;
        rule = null;
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.data.AbstractDbData#delete()
     */
    @Override
    public void delete() throws OpenR66DatabaseException {
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                DbConstant.admin.session);
        try {
            preparedStatement.createPrepareStatement("DELETE FROM " + table +
                    " WHERE " + Columns.REQUESTED.name() + " = ? AND " +
                    Columns.SPECIALID.name() + " = ? ");
            primaryKey[0].setValue(requestedHostId);
            primaryKey[1].setValue(specialId);
            setValues(preparedStatement, primaryKey);
            int count = preparedStatement.executeUpdate();
            if (count <= 0) {
                throw new OpenR66DatabaseNoDataException("No row found");
            }
            isSaved = false;
        } finally {
            preparedStatement.realClose();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.data.AbstractDbData#insert()
     */
    @Override
    public void insert() throws OpenR66DatabaseException {
        if (isSaved) {
            return;
        }
        // First need to find a new id if id is not ok
        if (specialId == DbConstant.ILLEGALVALUE) {
            specialId = DbModelFactory.dbModel.nextSequence();
            primaryKey[0].setValue(requestedHostId);
            primaryKey[1].setValue(specialId);
        }
        setToArray();
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                DbConstant.admin.session);
        try {
            preparedStatement.createPrepareStatement("INSERT INTO " + table +
                    " (" + selectAllFields + ") VALUES " + insertAllValues);
            setValues(preparedStatement, allFields);
            int count = preparedStatement.executeUpdate();
            if (count <= 0) {
                throw new OpenR66DatabaseNoDataException("No row found");
            }
            isSaved = true;
        } finally {
            preparedStatement.realClose();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.data.AbstractDbData#select()
     */
    @Override
    public void select() throws OpenR66DatabaseException {
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                DbConstant.admin.session);
        preparedStatement.createPrepareStatement("SELECT " + selectAllFields +
                " FROM " + table + " WHERE " + Columns.REQUESTED.name() +
                " = ? AND " + Columns.SPECIALID.name() + " = ? ");
        try {
            primaryKey[0].setValue(requestedHostId);
            primaryKey[1].setValue(specialId);
            setValues(preparedStatement, primaryKey);
            preparedStatement.executeQuery();
            if (preparedStatement.getNext()) {
                getValues(preparedStatement, allFields);
                setFromArray();
                isSaved = true;
            } else {
                throw new OpenR66DatabaseNoDataException("No row found");
            }
        } finally {
            preparedStatement.realClose();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.data.AbstractDbData#update()
     */
    @Override
    public void update() throws OpenR66DatabaseException {
        if (isSaved) {
            return;
        }
        setToArray();
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                DbConstant.admin.session);
        try {
            preparedStatement.createPrepareStatement("UPDATE " + table +
                    " SET " + updateAllFields + " WHERE " +
                    Columns.REQUESTED.name() + " = ? AND " +
                    Columns.SPECIALID.name() + " = ? ");
            setValues(preparedStatement, allFields);
            int count = preparedStatement.executeUpdate();
            if (count <= 0) {
                throw new OpenR66DatabaseNoDataException("No row found");
            }
            isSaved = true;
        } finally {
            preparedStatement.realClose();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.data.AbstractDbData#changeUpdatedInfo(int)
     */
    @Override
    public void changeUpdatedInfo(int status) {
        if (updatedInfo != status) {
            updatedInfo = status;
            allFields[Columns.UPDATEDINFO.ordinal()].setValue(updatedInfo);
            isSaved = false;
        }
    }

    /**
     * To set the rank at startup of the request if the request specify a
     * specific rank
     *
     * @param rank
     *            the rank to set
     */
    public void setRankAtStartup(int rank) {
        if (this.rank != rank) {
            this.rank = rank;
            allFields[Columns.RANK.ordinal()].setValue(this.rank);
            isSaved = false;
        }
    }

    /**
     * @param filename
     *            the filename to set
     */
    public void setFilename(String filename) {
        if (this.filename != filename) {
            this.filename = filename;
            allFields[Columns.FILENAME.ordinal()].setValue(this.filename);
            isSaved = false;
        }
    }

    /**
     * @param isFileMoved
     *            the isFileMoved to set
     */
    public void setFileMoved(boolean isFileMoved) {
        if (this.isFileMoved != isFileMoved) {
            this.isFileMoved = isFileMoved;
            allFields[Columns.ISMOVED.ordinal()].setValue(this.isFileMoved);
            isSaved = false;
        }
    }

    /**
     * @param originalFilename
     *            the originalFilename to set
     */
    public void setOriginalFilename(String originalFilename) {
        if (this.originalFilename != originalFilename) {
            this.originalFilename = originalFilename;
            allFields[Columns.ORIGINALNAME.ordinal()]
                    .setValue(this.originalFilename);
            isSaved = false;
        }
    }

    public RequestPacket getRequest() {
        // FIXME
        return new RequestPacket(ruleId, mode, originalFilename, blocksize,
                rank, specialId, fileInformation);
    }

    /**
     * @return the rank
     */
    public int getRank() {
        return rank;
    }

    /**
     *
     * @return True if the runner is currently in transfer
     */
    public boolean isInTransfer() {
        return globalstep == TRANSFERTASK;
    }

    /**
     * Change the status from Task Execution
     *
     * @param status
     */
    public void setExecutionStatus(TaskStatus status) {
        this.status = status;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.ordinal());
        isSaved = false;
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
    public DbR66Rule getRule() {
        return rule;
    }

    /**
     * @return the mode
     */
    public int getMode() {
        return mode;
    }

    /**
     * @return the globallaststep
     */
    public int getGloballaststep() {
        return globallaststep;
    }
    /**
     *
     * @return True if this runner is ready for transfer or post operation
     */
    public boolean ready() {
        return globalstep > PRETASK;
    }
    /**
     *
     * @return True if this runner is finished, either in success or in error
     */
    public boolean isFinished() {
        return globalstep == ALLDONETASK || globalstep == ERRORTASK &&
                status == TaskStatus.OK;
    }
    /**
     * Set Pre Task step
     * @param step
     */
    public void setPreTask(int step) {
        globalstep = PRETASK;
        globallaststep = PRETASK;
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(globalstep);
        allFields[Columns.GLOBALLASTSTEP.ordinal()].setValue(globallaststep);
        this.step = step;
        allFields[Columns.STEP.ordinal()].setValue(this.step);
        status = TaskStatus.RUNNING;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(status.ordinal());
        isSaved = false;
    }
    /**
     * Set Transfer rank
     * @param rank
     */
    public void setTransferTask(int rank) {
        globalstep = TRANSFERTASK;
        globallaststep = TRANSFERTASK;
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(globalstep);
        allFields[Columns.GLOBALLASTSTEP.ordinal()].setValue(globallaststep);
        this.rank = rank;
        allFields[Columns.RANK.ordinal()].setValue(this.rank);
        status = TaskStatus.RUNNING;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(status.ordinal());
        isSaved = false;
    }
    /**
     * Set the status of the transfer
     * @param status True if success
     * @return the currentrank of transfer
     */
    public int finishTransferTask(boolean status) {
        if (status) {
            this.status = TaskStatus.OK;
        } else {
            this.status = TaskStatus.ERROR;
        }
        allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.ordinal());
        isSaved = false;
        return rank;
    }
    /**
     * Set the Post Task step
     * @param step
     */
    public void setPostTask(int step) {
        globalstep = POSTTASK;
        globallaststep = POSTTASK;
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(globalstep);
        allFields[Columns.GLOBALLASTSTEP.ordinal()].setValue(globallaststep);
        this.step = step;
        allFields[Columns.STEP.ordinal()].setValue(this.step);
        status = TaskStatus.RUNNING;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(status.ordinal());
        isSaved = false;
    }
    /**
     * Set the Error Task step
     * @param step
     */
    public void setErrorTask(int step) {
        globalstep = ERRORTASK;
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(globalstep);
        this.step = step;
        allFields[Columns.STEP.ordinal()].setValue(this.step);
        status = TaskStatus.RUNNING;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(status.ordinal());
        isSaved = false;
    }
    /**
     * Set the global step as finished (after post task in success)
     */
    public void setAllDone() {
        globalstep = ALLDONETASK;
        globallaststep = ALLDONETASK;
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(globalstep);
        allFields[Columns.GLOBALLASTSTEP.ordinal()].setValue(globallaststep);
        step = 0;
        allFields[Columns.STEP.ordinal()].setValue(step);
        // FIXME set by POST status = TaskStatus.OK;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(status.ordinal());
        isSaved = false;
    }
    /**
     * Run the task from the given task information (from rule)
     * @param tasks
     * @return The future of the operation (in success or not)
     * @throws OpenR66RunnerEndTasksException
     * @throws OpenR66RunnerErrorException
     */
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
    /**
     *
     * @return the future of the task run
     * @throws OpenR66RunnerEndTasksException
     * @throws OpenR66RunnerErrorException
     */
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
    /**
     * Run all task from current status (globalstep and step)
     * @throws OpenR66RunnerErrorException
     */
    public void run() throws OpenR66RunnerErrorException {
        R66Future future;
        if (status != TaskStatus.RUNNING) {
            throw new OpenR66RunnerErrorException(
                    "Current global STEP not ready to run");
        }
        while (true) {
            try {
                future = runNext();
            } catch (OpenR66RunnerEndTasksException e) {
                if (status == TaskStatus.RUNNING) {
                    status = TaskStatus.OK;
                }
                allFields[Columns.STEP.ordinal()].setValue(step);
                allFields[Columns.STEPSTATUS.ordinal()].setValue(status
                        .ordinal());
                isSaved = false;
                return;
            } catch (OpenR66RunnerErrorException e) {
                status = TaskStatus.ERROR;
                allFields[Columns.STEP.ordinal()].setValue(step);
                allFields[Columns.STEPSTATUS.ordinal()].setValue(status
                        .ordinal());
                isSaved = false;
                throw new OpenR66RunnerErrorException("Runner is in error: " +
                        e.getMessage(), e);
            }
            if (future.isCancelled()) {
                status = TaskStatus.ERROR;
                allFields[Columns.STEP.ordinal()].setValue(step);
                allFields[Columns.STEPSTATUS.ordinal()].setValue(status
                        .ordinal());
                isSaved = false;
                throw new OpenR66RunnerErrorException("Runner is error: " +
                        future.getCause().getMessage(), future.getCause());
            }
            step ++;
        }
    }

    /**
     * Increment the rank of the transfer
     */
    public void incrementRank() {
        rank ++;
        allFields[Columns.RANK.ordinal()].setValue(rank);
        isSaved = false;
        if (rank % 10 == 0) {
            // Save each 10 blocks
            try {
                update();
            } catch (OpenR66DatabaseException e) {
                logger.warn("Cannot update Runner", e);
            }
        }
    }

    /**
     * This method is to be called each time an operation is happening on Runner
     *
     * @throws OpenR66RunnerErrorException
     */
    public void saveStatus() throws OpenR66RunnerErrorException {
        logger
                .info(GgInternalLogger.getRankMethodAndLine(3) + " " +
                        toString());
        try {
            update();
        } catch (OpenR66DatabaseException e) {
            throw new OpenR66RunnerErrorException(e);
        }
    }
    /**
     * Clear the runner
     */
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
     * TransferId to retrieve list of runners according to a status
     *
     * @author Frederic Bregier
     *
     */
    public static class TransferId {
        public long specialId;

        public String requestedHost;
    }

    /**
     *
     * @param status
     *            the status to match or UNKNOWN for all
     * @return All index (specialId,requestedHost) that match the status
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     * @throws OpenR66DatabaseNoDataException
     */
    public static ArrayList<TransferId> getAllRunner(int status)
            throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError,
            OpenR66DatabaseNoDataException {
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                DbConstant.admin.session);
        DbTaskRunner runner = null;
        runner = new DbTaskRunner();
        try {
            if (status == UpdatedInfo.UNKNOWN.ordinal()) {
                preparedStatement.createPrepareStatement("SELECT COUNT(" +
                        Columns.SPECIALID.name() + ") FROM " + table);
            } else {
                preparedStatement.createPrepareStatement("SELECT COUNT(" +
                        Columns.SPECIALID.name() + ") FROM " + table +
                        " WHERE " + Columns.UPDATEDINFO.name() + " = ?");
                runner.allFields[Columns.UPDATEDINFO.ordinal()]
                        .setValue(status);
                runner.setValue(preparedStatement,
                        runner.allFields[Columns.UPDATEDINFO.ordinal()]);
            }
            preparedStatement.executeQuery();
            int count = 0;
            if (preparedStatement.getNext()) {
                ResultSet rs = preparedStatement.getResultSet();
                try {
                    count = rs.getInt(1);
                } catch (SQLException e) {
                    DbSession.error(e);
                    throw new OpenR66DatabaseSqlError(
                            "Getting values in error: Integer for Count", e);
                }
            } else {
                throw new OpenR66DatabaseNoDataException("No row found");
            }
            preparedStatement.realClose();
            ArrayList<TransferId> result = new ArrayList<TransferId>(count);
            if (status == UpdatedInfo.UNKNOWN.ordinal()) {
                preparedStatement.createPrepareStatement("SELECT " +
                        Columns.REQUESTED.name() + "," +
                        Columns.SPECIALID.name() + " FROM " + table);
            } else {
                preparedStatement.createPrepareStatement("SELECT " +
                        Columns.REQUESTED.name() + "," +
                        Columns.SPECIALID.name() + " FROM " + table +
                        " WHERE " + Columns.UPDATEDINFO.name() + " = ?");
                runner.allFields[Columns.UPDATEDINFO.ordinal()]
                        .setValue(status);
                runner.setValue(preparedStatement,
                        runner.allFields[Columns.UPDATEDINFO.ordinal()]);
            }
            preparedStatement.executeQuery();
            while (preparedStatement.getNext()) {
                runner.getValues(preparedStatement, runner.primaryKey);
                TransferId id = new TransferId();
                id.requestedHost = (String) runner.primaryKey[0].value;
                id.specialId = (Long) runner.primaryKey[1].value;
                result.add(id);
            }
            return result;
        } finally {
            preparedStatement.realClose();
        }
    }
}
