/**
   This file is part of GoldenGate Project (named also GoldenGate or GG).

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All GoldenGate Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   GoldenGate is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GoldenGate .  If not, see <http://www.gnu.org/licenses/>.
 */
package openr66.database.data;

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.database.DbPreparedStatement;
import goldengate.common.database.DbSession;
import goldengate.common.database.data.AbstractDbData;
import goldengate.common.database.data.DbValue;
import goldengate.common.database.exception.OpenR66DatabaseException;
import goldengate.common.database.exception.OpenR66DatabaseNoConnectionError;
import goldengate.common.database.exception.OpenR66DatabaseNoDataException;
import goldengate.common.database.exception.OpenR66DatabaseSqlError;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.TreeSet;

import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.R66Session;
import openr66.context.filesystem.R66Dir;
import openr66.context.filesystem.R66File;
import openr66.context.task.AbstractTask;
import openr66.context.task.TaskType;
import openr66.context.task.exception.OpenR66RunnerEndTasksException;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.DbConstant;
import openr66.database.model.DbModelFactory;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolBusinessException;
import openr66.protocol.exception.OpenR66ProtocolNoSslException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.http.HttpFormattedHandler;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.localhandler.packet.RequestPacket.TRANSFERMODE;
import openr66.protocol.utils.FileUtils;
import openr66.protocol.utils.NbAndSpecialId;
import openr66.protocol.utils.R66Future;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import org.dom4j.tree.DefaultElement;
import org.xml.sax.SAXException;

/**
 * Task Runner from pre operation to transfer to post operation, except in case
 * of error
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
        BLOCKSZ,
        ORIGINALNAME,
        FILEINFO,
        MODETRANS,
        STARTTRANS,
        STOPTRANS,
        INFOSTATUS,
        UPDATEDINFO,
        OWNERREQ,
        REQUESTER,
        REQUESTED,
        SPECIALID;
    }

    public static int[] dbTypes = {
            Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER,
            Types.CHAR, Types.BIT, Types.VARCHAR, Types.BIT, Types.VARCHAR,
            Types.INTEGER, Types.VARCHAR, Types.LONGVARCHAR, Types.INTEGER,
            Types.TIMESTAMP, Types.TIMESTAMP, Types.CHAR, Types.INTEGER,
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.BIGINT };

    public static String table = " RUNNER ";

    public static String fieldseq = "RUNSEQ";

    public static Columns [] indexes = {
        Columns.STARTTRANS, Columns.OWNERREQ, Columns.STEPSTATUS, Columns.UPDATEDINFO,
        Columns.GLOBALSTEP
    };

    public static final String XMLRUNNERS = "taskrunners";
    public static final String XMLRUNNER = "runner";
    /**
     * GlobalStep Value
     */
    public static enum TASKSTEP {
        NOTASK, PRETASK, TRANSFERTASK, POSTTASK, ALLDONETASK, ERRORTASK;
    }

    // Values
    private DbRule rule;

    private R66Session session;

    /**
     * Last step
     */
    private int globalstep = TASKSTEP.NOTASK.ordinal();
    /**
     * Last global step (only changes in case of success)
     */
    private int globallaststep = TASKSTEP.NOTASK.ordinal();
    /**
     * Step in the current globalstep
     */
    private int step = -1;

    private int rank = 0;

    /**
     * Last step action status error code
     */
    private ErrorCode status = ErrorCode.Unknown;

    private long specialId;

    private boolean isSender;

    private String filename;

    private boolean isFileMoved = false;

    private String ruleId;

    private int blocksize;

    private String originalFilename;

    private String fileInformation;

    private int mode;

    private String ownerRequest;

    private String requesterHostId;

    private String requestedHostId;

    private Timestamp start;

    private Timestamp stop;

    /**
     * Info status error code
     */
    private ErrorCode infostatus = ErrorCode.Unknown;

    /**
     * The global status for running
     */
    private int updatedInfo = UpdatedInfo.UNKNOWN.ordinal();

    private boolean isSaved = false;

    private volatile boolean continueTransfer = true;

    /**
     * Special For DbTaskRunner
     */
    public static final int NBPRKEY = 4;
    // ALL TABLE SHOULD IMPLEMENT THIS
    private final DbValue primaryKey[] = {
            new DbValue(ownerRequest, Columns.OWNERREQ.name()),
            new DbValue(requesterHostId, Columns.REQUESTER.name()),
            new DbValue(requestedHostId, Columns.REQUESTED.name()),
            new DbValue(specialId, Columns.SPECIALID.name()) };
    private final DbValue[] otherFields = {
            // GLOBALSTEP, GLOBALLASTSTEP, STEP, RANK, STEPSTATUS, RETRIEVEMODE,
            // FILENAME, ISMOVED, IDRULE,
            // BLOCKSZ, ORIGINALNAME, FILEINFO, MODETRANS,
            // STARTTRANS, STOPTRANS
            // INFOSTATUS, UPDATEDINFO
            new DbValue(globalstep, Columns.GLOBALSTEP.name()),
            new DbValue(globallaststep, Columns.GLOBALLASTSTEP.name()),
            new DbValue(step, Columns.STEP.name()),
            new DbValue(rank, Columns.RANK.name()),
            new DbValue(status.getCode(), Columns.STEPSTATUS.name()),
            new DbValue(isSender, Columns.RETRIEVEMODE.name()),
            new DbValue(filename, Columns.FILENAME.name()),
            new DbValue(isFileMoved, Columns.ISMOVED.name()),
            new DbValue(ruleId, Columns.IDRULE.name()),
            new DbValue(blocksize, Columns.BLOCKSZ.name()),
            new DbValue(originalFilename, Columns.ORIGINALNAME.name()),
            new DbValue(fileInformation, Columns.FILEINFO.name()),
            new DbValue(mode, Columns.MODETRANS.name()),
            new DbValue(start, Columns.STARTTRANS.name()),
            new DbValue(stop, Columns.STOPTRANS.name()),
            new DbValue(infostatus.getCode(), Columns.INFOSTATUS.name()),
            new DbValue(updatedInfo, Columns.UPDATEDINFO.name()) };

    private final DbValue[] allFields = {
            otherFields[0], otherFields[1], otherFields[2], otherFields[3],
            otherFields[4], otherFields[5], otherFields[6], otherFields[7],
            otherFields[8], otherFields[9], otherFields[10], otherFields[11],
            otherFields[12], otherFields[13], otherFields[14], otherFields[15],
            otherFields[16],
            primaryKey[0], primaryKey[1], primaryKey[2], primaryKey[3]  };

    public static final String selectAllFields = Columns.GLOBALSTEP.name() +
            "," + Columns.GLOBALLASTSTEP.name() + "," + Columns.STEP.name() +
            "," + Columns.RANK.name() + "," + Columns.STEPSTATUS.name() + "," +
            Columns.RETRIEVEMODE.name() + "," + Columns.FILENAME.name() + "," +
            Columns.ISMOVED.name() + "," + Columns.IDRULE.name() + "," +
            Columns.BLOCKSZ.name() + "," + Columns.ORIGINALNAME.name() + "," +
            Columns.FILEINFO.name() + "," + Columns.MODETRANS.name() + "," +
            Columns.STARTTRANS.name() + "," + Columns.STOPTRANS.name() + "," +
            Columns.INFOSTATUS.name() + "," + Columns.UPDATEDINFO.name() + "," +
            Columns.OWNERREQ.name() + "," + Columns.REQUESTER.name() + "," +
            Columns.REQUESTED.name() + "," + Columns.SPECIALID.name();

    private static final String updateAllFields = Columns.GLOBALSTEP.name() +
            "=?," + Columns.GLOBALLASTSTEP.name() + "=?," +
            Columns.STEP.name() + "=?," + Columns.RANK.name() + "=?," +
            Columns.STEPSTATUS.name() + "=?," + Columns.RETRIEVEMODE.name() +
            "=?," + Columns.FILENAME.name() + "=?," + Columns.ISMOVED.name() +
            "=?," + Columns.IDRULE.name() + "=?," + Columns.BLOCKSZ.name() +
            "=?," + Columns.ORIGINALNAME.name() + "=?," +
            Columns.FILEINFO.name() + "=?," + Columns.MODETRANS.name() + "=?," +
            Columns.STARTTRANS.name() + "=?," + Columns.STOPTRANS.name() +
            "=?," + Columns.INFOSTATUS.name() + "=?," + Columns.UPDATEDINFO.name() + "=?";

    private static final String insertAllValues = " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";

    private static final TreeSet<Long> clientNoDbSpecialId = new TreeSet<Long>();
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
            try {
                return Configuration.configuration.getHostId(session.getAuth()
                        .isSsl());
            } catch (OpenR66ProtocolNoSslException e) {
                return Configuration.configuration.HOST_ID;
            }
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
            try {
                return Configuration.configuration.getHostId(session.getAuth()
                        .isSsl());
            } catch (OpenR66ProtocolNoSslException e) {
                return Configuration.configuration.HOST_ID;
            }
        }
    }

    /**
     * Constructor for submission (no transfer session), from database. It is
     * created, so with a new specialId if necessary
     *
     * @param dbSession
     * @param rule
     * @param isSender
     * @param requestPacket
     * @param requested
     * @throws OpenR66DatabaseException
     */
    public DbTaskRunner(DbSession dbSession, DbRule rule, boolean isSender,
            RequestPacket requestPacket, String requested)
            throws OpenR66DatabaseException {
        super(dbSession);
        this.session = null;
        this.rule = rule;
        ruleId = this.rule.idRule;
        rank = requestPacket.getRank();
        status = ErrorCode.Unknown;
        infostatus = ErrorCode.Unknown;
        this.isSender = isSender;
        filename = requestPacket.getFilename();
        blocksize = requestPacket.getBlocksize();
        originalFilename = requestPacket.getFilename();
        fileInformation = requestPacket.getFileInformation();
        mode = requestPacket.getMode();
        // itself but according to SSL
        requesterHostId = Configuration.configuration.getHostId(dbSession,
                requested);
        // given one
        requestedHostId = requested;
        // always itself
        ownerRequest = Configuration.configuration.HOST_ID;

        start = new Timestamp(System.currentTimeMillis());
        setToArray();
        isSaved = false;
        specialId = requestPacket.getSpecialId();
        if (this.rule == null) {
            this.rule = new DbRule(this.dbSession, ruleId);
        }
        if (mode != rule.mode) {
            if (RequestPacket.isMD5Mode(mode)) {
                mode = RequestPacket.getModeMD5(rule.mode);
            } else {
                mode = rule.mode;
            }
        }
        create();
    }

    /**
     * Constructor from a request with a valid Special Id to be inserted into database
     *
     * @param dbSession
     * @param session
     * @param rule
     * @param isSender
     * @param requestPacket
     * @throws OpenR66DatabaseException
     */
    public DbTaskRunner(DbSession dbSession, R66Session session, DbRule rule,
            boolean isSender, RequestPacket requestPacket)
            throws OpenR66DatabaseException {
        super(dbSession);
        this.session = session;
        this.rule = rule;
        ruleId = this.rule.idRule;
        rank = requestPacket.getRank();
        status = ErrorCode.Unknown;
        infostatus = ErrorCode.Unknown;
        this.isSender = isSender;
        filename = requestPacket.getFilename();
        blocksize = requestPacket.getBlocksize();
        originalFilename = requestPacket.getFilename();
        fileInformation = requestPacket.getFileInformation();
        mode = requestPacket.getMode();
        requesterHostId = getRequester(session, requestPacket);
        requestedHostId = getRequested(session, requestPacket);
        // always itself
        ownerRequest = Configuration.configuration.HOST_ID;

        start = new Timestamp(System.currentTimeMillis());
        setToArray();
        isSaved = false;
        specialId = requestPacket.getSpecialId();
        if (this.rule == null) {
            this.rule = new DbRule(this.dbSession, ruleId);
        }
        if (mode != rule.mode) {
            if (RequestPacket.isMD5Mode(mode)) {
                mode = RequestPacket.getModeMD5(rule.mode);
            } else {
                mode = rule.mode;
            }
        }
        insert();
    }

    /**
     * Constructor from a request with a valid Special Id so loaded from database
     *
     * @param dbSession
     * @param session
     * @param rule
     * @param id
     * @param requester
     * @param requested
     * @throws OpenR66DatabaseException
     */
    public DbTaskRunner(DbSession dbSession, R66Session session, DbRule rule,
            long id, String requester, String requested)
            throws OpenR66DatabaseException {
        super(dbSession);
        this.session = session;
        this.rule = rule;

        specialId = id;
        // retrieving a task should be made from the requester, but the caller
        // is responsible of this
        requestedHostId = requested;
        requesterHostId = requester;
        // always itself
        ownerRequest = Configuration.configuration.HOST_ID;

        select();

        if (rule != null) {
            if (!ruleId.equals(rule.idRule)) {
                throw new OpenR66DatabaseNoDataException(
                        "Rule does not correspond");
            }
        }
    }

    @Override
    protected void setToArray() {
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(globalstep);
        allFields[Columns.GLOBALLASTSTEP.ordinal()].setValue(globallaststep);
        allFields[Columns.STEP.ordinal()].setValue(step);
        allFields[Columns.RANK.ordinal()].setValue(rank);
        allFields[Columns.STEPSTATUS.ordinal()].setValue(status.getCode());
        allFields[Columns.RETRIEVEMODE.ordinal()].setValue(isSender);
        allFields[Columns.FILENAME.ordinal()].setValue(filename);
        allFields[Columns.ISMOVED.ordinal()].setValue(isFileMoved);
        allFields[Columns.IDRULE.ordinal()].setValue(ruleId);
        allFields[Columns.BLOCKSZ.ordinal()].setValue(blocksize);
        allFields[Columns.ORIGINALNAME.ordinal()].setValue(originalFilename);
        allFields[Columns.FILEINFO.ordinal()].setValue(fileInformation);
        allFields[Columns.MODETRANS.ordinal()].setValue(mode);
        allFields[Columns.STARTTRANS.ordinal()].setValue(start);
        stop = new Timestamp(System.currentTimeMillis());
        allFields[Columns.STOPTRANS.ordinal()].setValue(stop);
        allFields[Columns.INFOSTATUS.ordinal()].setValue(infostatus.getCode());
        allFields[Columns.UPDATEDINFO.ordinal()].setValue(updatedInfo);
        allFields[Columns.OWNERREQ.ordinal()].setValue(ownerRequest);
        allFields[Columns.REQUESTER.ordinal()].setValue(requesterHostId);
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
        status = ErrorCode.getFromCode((String) allFields[Columns.STEPSTATUS
                .ordinal()].getValue());
        isSender = (Boolean) allFields[Columns.RETRIEVEMODE.ordinal()]
                .getValue();
        filename = (String) allFields[Columns.FILENAME.ordinal()].getValue();
        isFileMoved = (Boolean) allFields[Columns.ISMOVED.ordinal()].getValue();
        ruleId = (String) allFields[Columns.IDRULE.ordinal()].getValue();
        blocksize = (Integer) allFields[Columns.BLOCKSZ.ordinal()].getValue();
        originalFilename = (String) allFields[Columns.ORIGINALNAME.ordinal()]
                .getValue();
        fileInformation = (String) allFields[Columns.FILEINFO.ordinal()]
                .getValue();
        mode = (Integer) allFields[Columns.MODETRANS.ordinal()].getValue();
        start = (Timestamp) allFields[Columns.STARTTRANS.ordinal()].getValue();
        stop = (Timestamp) allFields[Columns.STOPTRANS.ordinal()].getValue();
        infostatus = ErrorCode.getFromCode((String) allFields[Columns.INFOSTATUS
                                                          .ordinal()].getValue());
        updatedInfo = (Integer) allFields[Columns.UPDATEDINFO.ordinal()]
                .getValue();
        ownerRequest = (String) allFields[Columns.OWNERREQ.ordinal()]
                                             .getValue();
        requesterHostId = (String) allFields[Columns.REQUESTER.ordinal()]
                .getValue();
        requestedHostId = (String) allFields[Columns.REQUESTED.ordinal()]
                .getValue();
        specialId = (Long) allFields[Columns.SPECIALID.ordinal()].getValue();
    }
    /**
     *
     * @return The Where condition on Primary Key
     */
    private String getWherePrimaryKey() {
        return primaryKey[0].column + " = ? AND " +
            primaryKey[1].column + " = ? AND " +
            primaryKey[2].column + " = ? AND " +
            primaryKey[3].column + " = ? ";
    }
    /**
     * Set the primary Key as current value
     */
    private void setPrimaryKey() {
        primaryKey[0].setValue(ownerRequest);
        primaryKey[1].setValue(requesterHostId);
        primaryKey[2].setValue(requestedHostId);
        primaryKey[3].setValue(specialId);
    }
    /**
     *
     * @return the condition to limit access to the row concerned by the Host
     */
    private static String getLimitWhereCondition() {
        return " "+Columns.OWNERREQ + " = '"+Configuration.configuration.HOST_ID+"' ";
    }
    /**
     * Create a Special Id for NoDb client
     */
    private void createNoDbSpecialId() {
        synchronized (clientNoDbSpecialId) {
            // New SpecialId is not possible with No Database Model
            specialId = System.currentTimeMillis();
            Long newOne = specialId;
            while (clientNoDbSpecialId.contains(newOne)) {
                newOne = specialId++;
            }
            clientNoDbSpecialId.add(newOne);
        }
    }
    /**
     * Remove a Spcieal Id for NoDb Client
     */
    private void removeNoDbSpecialId() {
        synchronized (clientNoDbSpecialId) {
            Long oldOne = specialId;
            clientNoDbSpecialId.remove(oldOne);
        }
    }
    /*
     * (non-Javadoc)
     *
     * @see openr66.databaseold.data.AbstractDbData#delete()
     */
    @Override
    public void delete() throws OpenR66DatabaseException {
        if (dbSession == null) {
            removeNoDbSpecialId();
            if (Configuration.configuration.saveTaskRunnerWithNoDb) {
                deleteXmlWorkNoDb();
            }
            return;
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("DELETE FROM " + table +
                    " WHERE " + getWherePrimaryKey());
            setPrimaryKey();
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
     * @see openr66.databaseold.data.AbstractDbData#insert()
     */
    @Override
    public void insert() throws OpenR66DatabaseException {
        if (isSaved) {
            return;
        }
        if (dbSession == null) {
            if (specialId == DbConstant.ILLEGALVALUE) {
                // New SpecialId is not possible with No Database Model
                createNoDbSpecialId();
            }
            isSaved = true;
            if (Configuration.configuration.saveTaskRunnerWithNoDb) {
                try {
                    setToArray();
                    this.writeXmlWorkNoDb();
                } catch (OpenR66ProtocolBusinessException e) {
                    // Ignore
                }
            }
            return;
        }
        // First need to find a new id if id is not ok
        if (specialId == DbConstant.ILLEGALVALUE) {
            specialId = DbModelFactory.dbModel.nextSequence(dbSession);
            logger.debug("Try Insert create a new Id from sequence: " +
                    specialId);
            setPrimaryKey();
        }
        setToArray();
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
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

    /**
     * As insert but with the ability to change the SpecialId
     *
     * @throws OpenR66DatabaseException
     */
    public void create() throws OpenR66DatabaseException {
        if (isSaved) {
            return;
        }
        if (dbSession == null) {
            if (specialId == DbConstant.ILLEGALVALUE) {
                // New SpecialId is not possible with No Database Model
                createNoDbSpecialId();
            }
            isSaved = true;
            if (Configuration.configuration.saveTaskRunnerWithNoDb) {
                try {
                    setToArray();
                    this.writeXmlWorkNoDb();
                } catch (OpenR66ProtocolBusinessException e) {
                    // Ignore
                }
            }
            return;
        }
        // First need to find a new id if id is not ok
        if (specialId == DbConstant.ILLEGALVALUE) {
            specialId = DbModelFactory.dbModel.nextSequence(dbSession);
            logger.info("Try Insert create a new Id from sequence: " +
                    specialId);
            setPrimaryKey();
        }
        setToArray();
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("INSERT INTO " + table +
                    " (" + selectAllFields + ") VALUES " + insertAllValues);
            setValues(preparedStatement, allFields);
            try {
                int count = preparedStatement.executeUpdate();
                if (count <= 0) {
                    throw new OpenR66DatabaseNoDataException("No row found");
                }
            } catch (OpenR66DatabaseSqlError e) {
                logger.error("Problem while inserting", e);
                DbPreparedStatement find = new DbPreparedStatement(dbSession);
                try {
                    find.createPrepareStatement("SELECT MAX(" +
                            primaryKey[3].column + ") FROM " + table + " WHERE " +
                            primaryKey[0].column + " = ? AND " +
                            primaryKey[1].column + " = ? AND " +
                            primaryKey[2].column + " = ? AND " +
                            primaryKey[3].column + " != ? ");
                    setPrimaryKey();
                    setValues(find, primaryKey);
                    find.executeQuery();
                    if (find.getNext()) {
                        long result;
                        try {
                            result = find.getResultSet().getLong(1);
                        } catch (SQLException e1) {
                            throw new OpenR66DatabaseSqlError(e1);
                        }
                        specialId = result + 1;
                        DbModelFactory.dbModel.resetSequence(dbSession, specialId + 1);
                        setToArray();
                        preparedStatement.close();
                        setValues(preparedStatement, allFields);
                        int count = preparedStatement.executeUpdate();
                        if (count <= 0) {
                            throw new OpenR66DatabaseNoDataException("No row found");
                        }
                    } else {
                        throw new OpenR66DatabaseNoDataException("No row found");
                    }
                } finally {
                    find.realClose();
                }
            }
            isSaved = true;
        } finally {
            preparedStatement.realClose();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.databaseold.data.AbstractDbData#exist()
     */
    @Override
    public boolean exist() throws OpenR66DatabaseException {
        if (dbSession == null) {
            if (Configuration.configuration.saveTaskRunnerWithNoDb) {
                return existXmlWorkNoDb();
            }
            return false;
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("SELECT " +
                    primaryKey[3].column + " FROM " + table + " WHERE " +
                    getWherePrimaryKey());
            setPrimaryKey();
            setValues(preparedStatement, primaryKey);
            preparedStatement.executeQuery();
            return preparedStatement.getNext();
        } finally {
            preparedStatement.realClose();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.databaseold.data.AbstractDbData#select()
     */
    @Override
    public void select() throws OpenR66DatabaseException {
        if (dbSession == null) {
            if (Configuration.configuration.saveTaskRunnerWithNoDb) {
                try {
                    this.loadXmlWorkNoDb();
                    setFromArray();
                } catch (OpenR66ProtocolBusinessException e) {
                    throw new OpenR66DatabaseNoDataException("No file found");
                }
                if (rule == null) {
                    rule = new DbRule(this.dbSession, ruleId);
                }
                isSaved = true;
                return;
            }
            throw new OpenR66DatabaseNoDataException("No row found");
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("SELECT " + selectAllFields +
                    " FROM " + table + " WHERE " +
                    getWherePrimaryKey());
            setPrimaryKey();
            setValues(preparedStatement, primaryKey);
            preparedStatement.executeQuery();
            if (preparedStatement.getNext()) {
                getValues(preparedStatement, allFields);
                setFromArray();
                if (rule == null) {
                    rule = new DbRule(this.dbSession, ruleId);
                }
                isSaved = true;
            } else {
                throw new OpenR66DatabaseNoDataException("No row found: " +
                        primaryKey[1].getValueAsString() + ":" +
                        primaryKey[2].getValueAsString() + ":" +
                        primaryKey[3].getValueAsString());
            }
        } finally {
            preparedStatement.realClose();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.databaseold.data.AbstractDbData#update()
     */
    @Override
    public void update() throws OpenR66DatabaseException {
        if (isSaved) {
            return;
        }
        if (dbSession == null) {
            isSaved = true;
            if (Configuration.configuration.saveTaskRunnerWithNoDb) {
                try {
                    setToArray();
                    this.writeXmlWorkNoDb();
                } catch (OpenR66ProtocolBusinessException e) {
                    // Ignore
                }
            }
            return;
        }
        setToArray();
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("UPDATE " + table +
                    " SET " + updateAllFields + " WHERE " +
                    getWherePrimaryKey());
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

    /**
     * Private constructor for Commander only
     *
     * @param session
     */
    private DbTaskRunner(DbSession dBsession) {
        super(dBsession);
        session = null;
        rule = null;
    }

    /**
     * For instance from Commander when getting updated information
     *
     * @param preparedStatement
     * @return the next updated DbTaskRunner
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public static DbTaskRunner getFromStatement(
            DbPreparedStatement preparedStatement)
            throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        DbTaskRunner dbTaskRunner = new DbTaskRunner(preparedStatement
                .getDbSession());
        dbTaskRunner.getValues(preparedStatement, dbTaskRunner.allFields);
        dbTaskRunner.setFromArray();
        if (dbTaskRunner.rule == null) {
            try {
                dbTaskRunner.rule = new DbRule(dbTaskRunner.dbSession, dbTaskRunner.ruleId);
            } catch (OpenR66DatabaseException e) {
                throw new OpenR66DatabaseSqlError(e);
            }
        }
        dbTaskRunner.isSaved = true;
        return dbTaskRunner;
    }

    /**
     * @param session
     * @param status
     * @param limit limit the number of rows
     * @return the DbPreparedStatement for getting Runner according to status ordered by start
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public static DbPreparedStatement getStatusPrepareStament(
            DbSession session, ErrorCode status, int limit)
            throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        String request = "SELECT " + selectAllFields + " FROM " + table;
        if (status != null) {
            request += " WHERE " + Columns.STEPSTATUS.name() + " = '" +
                    status.getCode() + "' AND "+getLimitWhereCondition();
        } else {
            request += " WHERE "+getLimitWhereCondition();
        }
        request += " ORDER BY " + Columns.STARTTRANS.name() + " DESC ";
        request = DbModelFactory.dbModel.limitRequest(selectAllFields, request, limit);
        return new DbPreparedStatement(session, request);
    }

    /**
     * @param session
     * @param globalstep
     * @param limit limit the number of rows
     * @return the DbPreparedStatement for getting Runner according to
     *         globalstep ordered by start
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public static DbPreparedStatement getStepPrepareStament(DbSession session,
            TASKSTEP globalstep, int limit) throws OpenR66DatabaseNoConnectionError,
            OpenR66DatabaseSqlError {
        String request = "SELECT " + selectAllFields + " FROM " + table;
        if (globalstep != null) {
            request += " WHERE (" + Columns.GLOBALSTEP.name() + " = " +
                    globalstep.ordinal();
            if (globalstep == TASKSTEP.ERRORTASK) {
                request += " OR "+Columns.UPDATEDINFO.name() + " = "+
                    UpdatedInfo.INERROR.ordinal()+") AND ";
            } else {
                request += ") AND ";
            }
            request += getLimitWhereCondition();
        }
        request += " ORDER BY " + Columns.STARTTRANS.name() + " DESC ";
        request = DbModelFactory.dbModel.limitRequest(selectAllFields, request, limit);
        return new DbPreparedStatement(session, request);
    }

    /**
     *
     * @param preparedStatement
     * @param srcrequest
     * @param limit
     * @param orderby
     * @param startid
     * @param stopid
     * @param start
     * @param stop
     * @param rule
     * @param req
     * @param pending
     * @param transfer
     * @param error
     * @param done
     * @param all
     * @return The DbPreparedStatement already prepared according to select or
     *         delete command
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    private static DbPreparedStatement getFilterCondition(
            DbPreparedStatement preparedStatement, String srcrequest, int limit,
            String orderby, String startid, String stopid, Timestamp start, Timestamp stop, String rule,
            String req, boolean pending, boolean transfer, boolean error,
            boolean done, boolean all) throws OpenR66DatabaseNoConnectionError,
            OpenR66DatabaseSqlError {
        String request = srcrequest;
        if (startid == null && stopid == null &&
                start == null && stop == null && rule == null && req == null && all) {
            // finish
            if (limit > 0) {
                request = DbModelFactory.dbModel.limitRequest(selectAllFields,
                        request+orderby, limit);
            } else {
                request = request+orderby;
            }
            preparedStatement.createPrepareStatement(request);
            return preparedStatement;
        }
        request += " WHERE ";
        StringBuilder scondition = new StringBuilder();
        if (start != null & stop != null) {
            scondition.append(Columns.STARTTRANS.name());
            scondition.append(" >= ? AND ");
            scondition.append(Columns.STARTTRANS.name());
            scondition.append(" <= ? ");
        } else if (start != null) {
            scondition.append(Columns.STARTTRANS.name());
            scondition.append(" >= ? ");
        } else if (stop != null) {
            scondition.append(Columns.STARTTRANS.name());
            scondition.append(" <= ? ");
        }
        if (startid != null) {
            if (scondition.length() != 0) {
                scondition.append(" AND ");
            }
            scondition.append(Columns.SPECIALID.name());
            scondition.append(" >= ? ");
        }
        if (stopid != null) {
            if (scondition.length() != 0) {
                scondition.append(" AND ");
            }
            scondition.append(Columns.SPECIALID.name());
            scondition.append(" <= ? ");
        }
        if (rule != null) {
            if (scondition.length() != 0) {
                scondition.append(" AND ");
            }
            scondition.append(Columns.IDRULE.name());
            scondition.append(" LIKE '%");
            scondition.append(rule);
            scondition.append("%' ");
        }
        if (req != null) {
            if (scondition.length() != 0) {
                scondition.append(" AND ");
            }
            scondition.append("( ");
            scondition.append(Columns.REQUESTED.name());
            scondition.append(" LIKE '%");
            scondition.append(req);
            scondition.append("%' OR ");
            scondition.append(Columns.REQUESTER.name());
            scondition.append(" LIKE '%");
            scondition.append(req);
            scondition.append("%' )");
        }
        if (!all) {
            if (scondition.length() != 0) {
                scondition.append(" AND ");
            }
            scondition.append("( ");
            boolean hasone = false;
            if (pending) {
                scondition.append(Columns.UPDATEDINFO.name());
                scondition.append(" = ");
                scondition.append(UpdatedInfo.TOSUBMIT.ordinal());
                hasone = true;
            }
            if (transfer) {
                if (hasone) {
                    scondition.append(" OR ");
                }
                scondition.append("( ");
                scondition.append(Columns.UPDATEDINFO.name());
                scondition.append(" = ");
                scondition.append(UpdatedInfo.RUNNING.ordinal());
                scondition.append(" )");
                hasone = true;
            }
            if (error) {
                if (hasone) {
                    scondition.append(" OR ");
                }
                scondition.append(Columns.GLOBALSTEP.name());
                scondition.append(" = ");
                scondition.append(TASKSTEP.ERRORTASK.ordinal());
                scondition.append(" OR ");
                scondition.append(Columns.UPDATEDINFO.name());
                scondition.append(" = ");
                scondition.append(UpdatedInfo.INERROR.ordinal());
                scondition.append(" OR ");
                scondition.append(Columns.UPDATEDINFO.name());
                scondition.append(" = ");
                scondition.append(UpdatedInfo.INTERRUPTED.ordinal());
                hasone = true;
            }
            if (done) {
                if (hasone) {
                    scondition.append(" OR ");
                }
                scondition.append(Columns.GLOBALSTEP.name());
                scondition.append(" = ");
                scondition.append(TASKSTEP.ALLDONETASK.ordinal());
                scondition.append(" OR ");
                scondition.append(Columns.UPDATEDINFO.name());
                scondition.append(" = ");
                scondition.append(UpdatedInfo.DONE.ordinal());
            }
            scondition.append(" )");
        }
        if (limit > 0) {
            scondition.insert(0, request);
            scondition.append(orderby);
            request = scondition.toString();
            request = DbModelFactory.dbModel.limitRequest(selectAllFields,
                request, limit);
        } else {
            scondition.insert(0, request);
            scondition.append(orderby);
            request = scondition.toString();
        }
        preparedStatement.createPrepareStatement(request);
        int rank = 1;
        try {
            if (start != null & stop != null) {
                preparedStatement.getPreparedStatement().setTimestamp(rank,
                        start);
                rank ++;
                preparedStatement.getPreparedStatement().setTimestamp(rank,
                        stop);
                rank ++;
            } else if (start != null) {
                preparedStatement.getPreparedStatement().setTimestamp(rank,
                        start);
                rank ++;
            } else if (stop != null) {
                preparedStatement.getPreparedStatement().setTimestamp(rank,
                        stop);
                rank ++;
            }
            if (startid != null) {
                long value = Long.parseLong(startid);
                preparedStatement.getPreparedStatement().setLong(rank,
                        value);
                rank ++;
            }
            if (stopid != null) {
                long value = Long.parseLong(stopid);
                preparedStatement.getPreparedStatement().setLong(rank,
                        value);
                rank ++;
            }
        } catch (SQLException e) {
            preparedStatement.realClose();
            throw new OpenR66DatabaseSqlError(e);
        }
        return preparedStatement;
    }

    /**
     *
     * @param session
     * @param limit
     * @param orderBySpecialId
     * @param startid
     * @param stopid
     * @param start
     * @param stop
     * @param rule
     * @param req
     * @param pending
     * @param transfer
     * @param error
     * @param done
     * @param all
     * @return the DbPreparedStatement according to the filter
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public static DbPreparedStatement getFilterPrepareStament(
            DbSession session, int limit, boolean orderBySpecialId, String startid, String stopid,
            Timestamp start, Timestamp stop, String rule,
            String req, boolean pending, boolean transfer, boolean error,
            boolean done, boolean all) throws OpenR66DatabaseNoConnectionError,
            OpenR66DatabaseSqlError {
        DbPreparedStatement preparedStatement = new DbPreparedStatement(session);
        String request = "SELECT " + selectAllFields + " FROM " + table;
        String orderby;
        if (startid == null && stopid == null &&
                start == null && stop == null && rule == null && req == null && all) {
            orderby = " WHERE "+getLimitWhereCondition();
        } else {
            orderby = " AND " + getLimitWhereCondition();
        }
        if (orderBySpecialId) {
            orderby += " ORDER BY " + Columns.SPECIALID.name() + " DESC ";
        } else {
            orderby += " ORDER BY " + Columns.STARTTRANS.name() + " DESC ";
        }
        return getFilterCondition(preparedStatement, request, limit, orderby,
                startid, stopid, start, stop, rule,
                req, pending, transfer, error, done, all);
    }

    /**
     *
     * @param session
     * @param info
     * @param orderByStart
     * @param limit
     * @return the DbPreparedStatement for getting Updated Object
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public static DbPreparedStatement getUpdatedPrepareStament(DbSession session,
            UpdatedInfo info, boolean orderByStart, int limit)
            throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        String request = "SELECT " + selectAllFields+
                " FROM " + table + " WHERE " + Columns.UPDATEDINFO.name() +
                " = " + info.ordinal()+ " AND "+getLimitWhereCondition();
        //FIXME if adding limit after is efficient with Oracle, remove the following code
        /*if (limit > 0) {
            if (DbModelFactory.dbModel instanceof DbModelOracle) {
                request += " AND ROWNUM <= "+limit;
            }
        }*/
        if (orderByStart) {
            request += " ORDER BY " + Columns.STARTTRANS.name() + " DESC ";
        }
        /*if (limit > 0 &&
                (!(DbModelFactory.dbModel instanceof DbModelOracle))) {*/
            request =
                DbModelFactory.dbModel.limitRequest(selectAllFields, request, limit);
        /*}*/
        return new DbPreparedStatement(session, request);
    }

    /**
     *
     * @param session
     * @param start
     * @param stop
     * @return the DbPreparedStatement for getting Selected Object, whatever their status
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public static DbPreparedStatement getLogPrepareStament(DbSession session,
            Timestamp start, Timestamp stop)
            throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        DbPreparedStatement preparedStatement = new DbPreparedStatement(session);
        String request = "SELECT " + selectAllFields + " FROM " + table;
        if (start != null & stop != null) {
            request += " WHERE " + Columns.STARTTRANS.name() + " >= ? AND " +
                    Columns.STARTTRANS.name() + " <= ? AND "+getLimitWhereCondition()+
                    " ORDER BY " + Columns.SPECIALID.name() + " DESC ";
            preparedStatement.createPrepareStatement(request);
            try {
                preparedStatement.getPreparedStatement().setTimestamp(1, start);
                preparedStatement.getPreparedStatement().setTimestamp(2, stop);
            } catch (SQLException e) {
                preparedStatement.realClose();
                throw new OpenR66DatabaseSqlError(e);
            }
        } else if (start != null) {
            request += " WHERE " + Columns.STARTTRANS.name() +
                    " >= ? AND "+getLimitWhereCondition()+
                    " ORDER BY " + Columns.SPECIALID.name() + " DESC ";
            preparedStatement.createPrepareStatement(request);
            try {
                preparedStatement.getPreparedStatement().setTimestamp(1, start);
            } catch (SQLException e) {
                preparedStatement.realClose();
                throw new OpenR66DatabaseSqlError(e);
            }
        } else if (stop != null) {
            request += " WHERE " + Columns.STARTTRANS.name() +
                    " <= ? AND "+getLimitWhereCondition()+
                    " ORDER BY " + Columns.SPECIALID.name() + " DESC ";
            preparedStatement.createPrepareStatement(request);
            try {
                preparedStatement.getPreparedStatement().setTimestamp(1, stop);
            } catch (SQLException e) {
                preparedStatement.realClose();
                throw new OpenR66DatabaseSqlError(e);
            }
        } else {
            request += " WHERE "+getLimitWhereCondition()+
                " ORDER BY " + Columns.SPECIALID.name() + " DESC ";
            preparedStatement.createPrepareStatement(request);
        }
        return preparedStatement;
    }

    /**
     * purge in same interval all runners with globallaststep as ALLDONETASK or
     * UpdatedInfo as Done
     *
     * @param session
     * @param start
     * @param stop
     * @return the number of log purged
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public static int purgeLogPrepareStament(DbSession session,
            Timestamp start, Timestamp stop)
            throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        DbPreparedStatement preparedStatement = new DbPreparedStatement(session);
        String request = "DELETE FROM " + table + " WHERE (" +
                Columns.GLOBALLASTSTEP + " = " +TASKSTEP.ALLDONETASK.ordinal() + " OR " +
                Columns.UPDATEDINFO + " = " +UpdatedInfo.DONE.ordinal() +
                ") AND "+getLimitWhereCondition();
        try {
            if (start != null & stop != null) {
                request += " AND " + Columns.STARTTRANS.name() + " >= ? AND " +
                        Columns.STOPTRANS.name() + " <= ? ";
                preparedStatement.createPrepareStatement(request);
                try {
                    preparedStatement.getPreparedStatement().setTimestamp(1, start);
                    preparedStatement.getPreparedStatement().setTimestamp(2, stop);
                } catch (SQLException e) {
                    preparedStatement.realClose();
                    throw new OpenR66DatabaseSqlError(e);
                }
            } else if (start != null) {
                request += " AND " + Columns.STARTTRANS.name() + " >= ? ";
                preparedStatement.createPrepareStatement(request);
                try {
                    preparedStatement.getPreparedStatement().setTimestamp(1, start);
                } catch (SQLException e) {
                    preparedStatement.realClose();
                    throw new OpenR66DatabaseSqlError(e);
                }
            } else if (stop != null) {
                request += " AND " + Columns.STOPTRANS.name() + " <= ? ";
                preparedStatement.createPrepareStatement(request);
                try {
                    preparedStatement.getPreparedStatement().setTimestamp(1, stop);
                } catch (SQLException e) {
                    preparedStatement.realClose();
                    throw new OpenR66DatabaseSqlError(e);
                }
            } else {
                preparedStatement.createPrepareStatement(request);
            }
            int nb = preparedStatement.executeUpdate();
            logger.info("Purge " + nb + " from " + request);
            return nb;
        } finally {
            preparedStatement.realClose();
        }
    }

    /**
     *
     * @param session
     * @param startid
     * @param stopid
     * @param start
     * @param stop
     * @param rule
     * @param req
     * @param pending
     * @param transfer
     * @param error
     * @param done
     * @param all
     * @return the DbPreparedStatement according to the filter and
     * ALLDONE, ERROR globallaststep
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public static int purgeLogPrepareStament(
            DbSession session, String startid, String stopid,
            Timestamp start, Timestamp stop, String rule,
            String req, boolean pending, boolean transfer, boolean error,
            boolean done, boolean all) throws OpenR66DatabaseNoConnectionError,
            OpenR66DatabaseSqlError {
        DbPreparedStatement preparedStatement = new DbPreparedStatement(session);
        String request = "DELETE FROM " + table;
        String orderby;
        if (startid == null && stopid == null && start == null && stop == null &&
                rule == null && req == null && all) {
            orderby = " WHERE (" +
                Columns.GLOBALLASTSTEP + " = " +TASKSTEP.ALLDONETASK.ordinal() + " OR " +
                Columns.UPDATEDINFO + " = " +UpdatedInfo.DONE.ordinal() +
                ") AND "+getLimitWhereCondition();
        } else {
            if (all) {
                orderby = " AND (" +
                    Columns.GLOBALLASTSTEP + " = " +TASKSTEP.ALLDONETASK.ordinal() + " OR " +
                    Columns.UPDATEDINFO + " = " +UpdatedInfo.DONE.ordinal() + " OR " +
                    Columns.UPDATEDINFO + " = " +UpdatedInfo.INERROR.ordinal() +
                    ") AND "+getLimitWhereCondition();
            } else {
                orderby = " AND "+
                    Columns.UPDATEDINFO + " <> " +UpdatedInfo.RUNNING.ordinal()+
                    " AND "+getLimitWhereCondition();// limit by field
            }
        }
        int nb = 0;
        try {
            preparedStatement = getFilterCondition(preparedStatement, request, 0,
                    orderby, startid, stopid, start, stop, rule,
                    req, pending, transfer, error, done, all);
            nb = preparedStatement.executeUpdate();
            logger.info("Purge " + nb + " from " + request);
        } finally {
            if (preparedStatement != null) {
                preparedStatement.realClose();
            }
        }
        return nb;
    }

    /**
     * Change RUNNING, INTERRUPTED to TOSUBMIT TaskRunner from database.
     * This method is to be used when the commander is starting the very first time,
     * in order to be ready to rerun tasks that are pending.
     *
     * @param session
     * @throws OpenR66DatabaseNoConnectionError
     */
    public static void resetToSubmit(DbSession session)
            throws OpenR66DatabaseNoConnectionError {
        // Change RUNNING and INTERRUPTED to TOSUBMIT since they should be ready
        String request = "UPDATE " + table + " SET " +
                Columns.UPDATEDINFO.name() + "=" +
                AbstractDbData.UpdatedInfo.TOSUBMIT.ordinal()+
                " WHERE (" + Columns.UPDATEDINFO.name() + " = " +
                AbstractDbData.UpdatedInfo.RUNNING.ordinal() +
                " OR "+ Columns.UPDATEDINFO.name() + " = " +
                AbstractDbData.UpdatedInfo.INTERRUPTED.ordinal()+") AND "+
                getLimitWhereCondition();
        DbPreparedStatement initial = new DbPreparedStatement(session);
        try {
            initial.createPrepareStatement(request);
            initial.executeUpdate();
        } catch (OpenR66DatabaseNoConnectionError e) {
            logger.error("Database No Connection Error: Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseSqlError e) {
            logger.error("Database SQL Error: Cannot execute Commander", e);
            return;
        } finally {
            initial.close();
        }
    }

    /**
     * Change CompleteOk+ALLDONETASK to Updated = DONE TaskRunner from database.
     * This method is a clean function to be used for instance before log export or
     * at the very beginning of the commander.
     *
     * @param session
     * @throws OpenR66DatabaseNoConnectionError
     */
    public static void changeFinishedToDone(DbSession session)
            throws OpenR66DatabaseNoConnectionError {
        // Update all UpdatedInfo to DONE where GlobalLastStep = ALLDONETASK and
        // status = CompleteOk
        String request = "UPDATE " + table + " SET " +
                Columns.UPDATEDINFO.name() + "=" +
                AbstractDbData.UpdatedInfo.DONE.ordinal()+
                " WHERE " + Columns.UPDATEDINFO.name() + " <> " +
                AbstractDbData.UpdatedInfo.DONE.ordinal() + " AND " +
                Columns.UPDATEDINFO.name() + " > 0 AND " +
                Columns.GLOBALLASTSTEP.name() + " = " +
                TASKSTEP.ALLDONETASK.ordinal() + " AND " +
                Columns.STEPSTATUS.name() + " = '" +
                ErrorCode.CompleteOk.getCode() + "' AND "+
                getLimitWhereCondition();
        DbPreparedStatement initial = new DbPreparedStatement(session);
        try {
            initial.createPrepareStatement(request);
            initial.executeUpdate();
        } catch (OpenR66DatabaseNoConnectionError e) {
            logger.error("Database No Connection Error: Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseSqlError e) {
            logger.error("Database SQL Error: Cannot execute Commander", e);
            return;
        } finally {
            initial.realClose();
        }
    }

    /**
     * Reset the runner (ready to be run again)
     *
     * @return True if OK, False if already finished
     */
    public boolean reset() {
        // Reset the status if already stopped and not finished
        if (this.getStatus() != ErrorCode.CompleteOk) {
            // restart
            switch (TASKSTEP.values()[this.getGloballaststep()]) {
                case PRETASK:
                    // restart
                    this.setPreTask();
                    this.setExecutionStatus(ErrorCode.InitOk);
                    break;
                case TRANSFERTASK:
                    // continue
                    int newrank = this.getRank();
                    this.setTransferTask(newrank);
                    this.setExecutionStatus(ErrorCode.PreProcessingOk);
                    break;
                case POSTTASK:
                    // restart
                    this.setPostTask();
                    this.setExecutionStatus(ErrorCode.TransferOk);
                    break;
            }
            this.changeUpdatedInfo(UpdatedInfo.UNKNOWN);
            this.setErrorExecutionStatus(this.status);
            return true;
        } else {
            // Already finished
            return false;
        }
    }

    /**
     * Make this Runner ready for restart
     * @param submit True to resubmit this task, else False to keep it as running (only reset)
     * @return True if OK or False if Already finished or if submitted and the request is
     *          a selfRequested and is not ready to restart locally
     * @throws OpenR66RunnerErrorException
     */
    public boolean restart(boolean submit) throws OpenR66RunnerErrorException {
        // Restart if not Requested
        if (submit) {
            if (isSelfRequested() && (this.globallaststep < TASKSTEP.POSTTASK.ordinal())) {
                return false;
            }
        }
        // Restart if already stopped and not finished
        if (reset()) {
            if ((!submit) && (this.globalstep == TASKSTEP.TRANSFERTASK.ordinal()) &&
                    (! this.isSender)) {
                int newrank = this.getRank();
                if (! this.isSender) {
                    if (newrank > 0) {
                        newrank -= Configuration.RANKRESTART;
                        if (newrank <= 0) {
                            newrank = 1;
                        }
                    }
                }
                this.setTransferTask(newrank);
            }
            if (submit) {
                this.changeUpdatedInfo(UpdatedInfo.TOSUBMIT);
            } else {
                this.changeUpdatedInfo(UpdatedInfo.RUNNING);
            }
            this.saveStatus();
            return true;
        } else {
            // Already finished so DONE
            this.setAllDone();
            this.setErrorExecutionStatus(ErrorCode.QueryAlreadyFinished);
            try {
                this.saveStatus();
            } catch (OpenR66RunnerErrorException e) {
            }
            return false;
        }
    }

    /**
     * Stop or Cancel a Runner from database point of view
     * @param code
     * @return True if correctly stopped or canceled
     */
    public boolean stopOrCancelRunner(ErrorCode code) {
        try {
            if (! isFinished()) {
                reset();
                switch (code) {
                    case CanceledTransfer:
                    case StoppedTransfer:
                    case RemoteShutdown:
                        this.changeUpdatedInfo(UpdatedInfo.INERROR);
                        break;
                    default:
                        this.changeUpdatedInfo(UpdatedInfo.INTERRUPTED);
                }
                update();
                logger.warn("StopOrCancel: {}\n    {}",code.mesg,this.toShortString());
                return true;
            } else {
                // is finished so do nothing
            }
        } catch (OpenR66DatabaseException e) {
        }
        return false;
    }
    /*
     * (non-Javadoc)
     *
     * @see openr66.databaseold.data.AbstractDbData#changeUpdatedInfo(UpdatedInfo)
     */
    @Override
    public void changeUpdatedInfo(UpdatedInfo info) {
        updatedInfo = info.ordinal();
        allFields[Columns.UPDATEDINFO.ordinal()].setValue(updatedInfo);
        isSaved = false;
    }
    /**
     * Set the ErrorCode for the UpdatedInfo
     * @param code
     */
    public void setErrorExecutionStatus(ErrorCode code) {
        if (infostatus != code) {
            infostatus = code;
            allFields[Columns.INFOSTATUS.ordinal()].setValue(infostatus.getCode());
            isSaved = false;
        }
    }
    /**
     *
     * @return The current UpdatedInfo value
     */
    public UpdatedInfo getUpdatedInfo() {
        return UpdatedInfo.values()[updatedInfo];
    }
    /**
     *
     * @return the error code associated with the Updated Info
     */
    public ErrorCode getErrorInfo() {
        return infostatus;
    }
    /**
     * To set the rank at startup of the request if the request specify a
     * specific rank
     *
     * @param rank
     *            the rank to set
     */
    public void setRankAtStartup(int rank) {
        if (this.rank > rank) {
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
        if (!this.filename.equals(filename)) {
            this.filename = filename;
            allFields[Columns.FILENAME.ordinal()].setValue(this.filename);
            isSaved = false;
        }
    }

    /**
     * @param newFilename
     *            the new Filename to set
     * @param isFileMoved
     *            the isFileMoved to set
     */
    public void setFileMoved(String newFilename, boolean isFileMoved) {
        if (this.isFileMoved != isFileMoved) {
            this.isFileMoved = isFileMoved;
            allFields[Columns.ISMOVED.ordinal()].setValue(this.isFileMoved);
            isSaved = false;
        }
        this.setFilename(newFilename);
    }

    /**
     * @param originalFilename
     *            the originalFilename to set
     */
    public void setOriginalFilename(String originalFilename) {
        if (!this.originalFilename.equals(originalFilename)) {
            this.originalFilename = originalFilename;
            allFields[Columns.ORIGINALNAME.ordinal()]
                    .setValue(this.originalFilename);
            isSaved = false;
        }
    }

    /**
     * @return the rank
     */
    public int getRank() {
        return rank;
    }

    /**
     * Change the status from Task Execution
     *
     * @param status
     */
    public void setExecutionStatus(ErrorCode status) {
        this.status = status;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.getCode());
        isSaved = false;
    }

    /**
     * @return the status
     */
    public ErrorCode getStatus() {
        return status;
    }

    /**
     * @return the isSender
     */
    public boolean isSender() {
        return isSender;
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
     * @return the filename
     */
    public String getFilename() {
        return filename;
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
    public DbRule getRule() {
        if (rule == null) {
            if (ruleId != null) {
                try {
                    rule = new DbRule(dbSession, ruleId);
                } catch (OpenR66DatabaseException e) {
                }
            }
        }
        return rule;
    }

    /**
     * @return the ruleId
     */
    public String getRuleId() {
        return ruleId;
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
        return globalstep > TASKSTEP.PRETASK.ordinal();
    }

    /**
    *
    * @return True if the runner is currently in transfer
    */
   public boolean isInTransfer() {
       return globalstep == TASKSTEP.TRANSFERTASK.ordinal();
   }

    /**
     *
     * @return True if this runner is finished, either in success or in error
     */
    public boolean isFinished() {
        return isAllDone() || isInError();
    }

    /**
     *
     * @return True if this runner is in error and no more running
     */
    public boolean isInError() {
        return (globalstep == TASKSTEP.ERRORTASK.ordinal() && status != ErrorCode.Running);
    }

    /**
     *
     * @return True if the runner is finished in success
     */
    public boolean isAllDone() {
        return globalstep == TASKSTEP.ALLDONETASK.ordinal();
    }

    /**
     * To be called before executing Pre execution
     * @return True if the task is going to run PRE task from the first action
     */
    public boolean isPreTaskStarting() {
        if (globallaststep == TASKSTEP.PRETASK.ordinal() ||
                globallaststep == TASKSTEP.NOTASK.ordinal()) {
            return (step-1 <= 0);
        }
        return false;
    }
    /**
     * Set Pre Task step
     *
     */
    public void setPreTask() {
        globalstep = TASKSTEP.PRETASK.ordinal();
        globallaststep = TASKSTEP.PRETASK.ordinal();
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(globalstep);
        allFields[Columns.GLOBALLASTSTEP.ordinal()].setValue(globallaststep);
        if (step <= 0) {
            this.step = 0;
        } else {
            this.step--;
        }
        allFields[Columns.STEP.ordinal()].setValue(this.step);
        status = ErrorCode.Running;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(status.getCode());
        this.changeUpdatedInfo(UpdatedInfo.RUNNING);
        this.setErrorExecutionStatus(ErrorCode.InitOk);
        isSaved = false;
    }

    /**
     * Set Transfer rank
     *
     * @param rank
     */
    public void setTransferTask(int rank) {
        globalstep = TASKSTEP.TRANSFERTASK.ordinal();
        globallaststep = TASKSTEP.TRANSFERTASK.ordinal();
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(globalstep);
        allFields[Columns.GLOBALLASTSTEP.ordinal()].setValue(globallaststep);
        if (this.rank > rank) {
            this.rank = rank;
        }
        allFields[Columns.RANK.ordinal()].setValue(this.rank);
        status = ErrorCode.Running;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(status.getCode());
        this.setErrorExecutionStatus(ErrorCode.PreProcessingOk);
        isSaved = false;
    }

    /**
     * Set the status of the transfer
     *
     * @param code
     *            TransferOk if success
     * @return the current rank of transfer
     */
    public int finishTransferTask(ErrorCode code) {
        if (code == ErrorCode.TransferOk) {
            this.status = code;
            this.setErrorExecutionStatus(code);
        } else {
            continueTransfer = false;
            if (this.infostatus == ErrorCode.InitOk ||
                    this.infostatus == ErrorCode.PostProcessingOk ||
                    this.infostatus == ErrorCode.PreProcessingOk ||
                    this.infostatus == ErrorCode.Running ||
                    this.infostatus == ErrorCode.TransferOk) {
                this.setErrorExecutionStatus(code);
            }
            if (this.updatedInfo != UpdatedInfo.INTERRUPTED.ordinal()) {
                this.changeUpdatedInfo(UpdatedInfo.INERROR);
            }
        }
        allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.getCode());
        isSaved = false;
        return rank;
    }
    /**
     *
     * @return True if the transfer is valid to continue
     */
    public boolean continueTransfer() {
        return continueTransfer;
    }
    /**
     * Set the Post Task step
     *
     */
    public void setPostTask() {
        globalstep = TASKSTEP.POSTTASK.ordinal();
        globallaststep = TASKSTEP.POSTTASK.ordinal();
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(globalstep);
        allFields[Columns.GLOBALLASTSTEP.ordinal()].setValue(globallaststep);
        if (step <= 0) {
            this.step = 0;
        } else {
            this.step--;
        }
        allFields[Columns.STEP.ordinal()].setValue(this.step);
        status = ErrorCode.Running;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(status.getCode());
        this.setErrorExecutionStatus(ErrorCode.TransferOk);
        isSaved = false;
    }

    /**
     * Set the Error Task step
     *
     */
    public void setErrorTask() {
        globalstep = TASKSTEP.ERRORTASK.ordinal();
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(globalstep);
        this.step = 0;
        allFields[Columns.STEP.ordinal()].setValue(this.step);
        status = ErrorCode.Running;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(status.getCode());
        isSaved = false;
    }

    /**
     * Set the global step as finished (after post task in success)
     */
    public void setAllDone() {
        globalstep = TASKSTEP.ALLDONETASK.ordinal();
        globallaststep = TASKSTEP.ALLDONETASK.ordinal();
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(globalstep);
        allFields[Columns.GLOBALLASTSTEP.ordinal()].setValue(globallaststep);
        step = 0;
        allFields[Columns.STEP.ordinal()].setValue(step);
        status = ErrorCode.CompleteOk;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(status.getCode());
        infostatus = ErrorCode.CompleteOk;
        allFields[Columns.INFOSTATUS.ordinal()].setValue(infostatus.getCode());
        changeUpdatedInfo(UpdatedInfo.DONE);
        isSaved = false;
    }

    /**
     * Run the task from the given task information (from rule)
     *
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
        try {
            task.getFutureCompletion().await();
        } catch (InterruptedException e) {
        }
        return task.getFutureCompletion();
    }

    /**
     *
     * @return the future of the task run
     * @throws OpenR66RunnerEndTasksException
     * @throws OpenR66RunnerErrorException
     * @throws OpenR66RunnerEndTasksException
     */
    private R66Future runNext() throws OpenR66RunnerErrorException,
            OpenR66RunnerEndTasksException {
        if (rule == null) {
            if (ruleId != null) {
                try {
                    rule = new DbRule(dbSession, ruleId);
                } catch (OpenR66DatabaseException e) {
                    rule = null;
                }
            }
            if (rule == null) {
                throw new OpenR66RunnerErrorException("Rule Object not initialized");
            }
        }
        switch (TASKSTEP.values()[globalstep]) {
            case PRETASK:
                try {
                    if (this.isSender) {
                        return runNextTask(rule.spreTasksArray);
                    } else {
                        return runNextTask(rule.rpreTasksArray);
                    }
                } catch (OpenR66RunnerEndTasksException e) {
                    if (status == ErrorCode.Running) {
                        infostatus = status = ErrorCode.PreProcessingOk;
                    }
                    throw e;
                }
            case POSTTASK:
                try {
                    if (this.isSender) {
                        return runNextTask(rule.spostTasksArray);
                    } else {
                        return runNextTask(rule.rpostTasksArray);
                    }
                } catch (OpenR66RunnerEndTasksException e) {
                    if (status == ErrorCode.Running) {
                        infostatus = status = ErrorCode.PostProcessingOk;
                    }
                    throw e;
                }
            case ERRORTASK:
                try {
                    if (this.isSender) {
                        return runNextTask(rule.serrorTasksArray);
                    } else {
                        return runNextTask(rule.rerrorTasksArray);
                    }
                } catch (OpenR66RunnerEndTasksException e) {
                    throw e;
                }
            default:
                throw new OpenR66RunnerErrorException("Global Step unknown");
        }
    }

    /**
     * Run all task from current status (globalstep and step)
     *
     * @throws OpenR66RunnerErrorException
     */
    public void run() throws OpenR66RunnerErrorException {
        R66Future future;
        if (status != ErrorCode.Running) {
            throw new OpenR66RunnerErrorException(
                    "Current global STEP not ready to run");
        }
        while (true) {
            try {
                future = runNext();
            } catch (OpenR66RunnerEndTasksException e) {
                step = 0;
                allFields[Columns.STEP.ordinal()].setValue(step);
                allFields[Columns.STEPSTATUS.ordinal()].setValue(status
                        .getCode());
                allFields[Columns.INFOSTATUS.ordinal()].setValue(infostatus.getCode());
                isSaved = false;
                this.saveStatus();
                return;
            } catch (OpenR66RunnerErrorException e) {
                infostatus = ErrorCode.ExternalOp;
                allFields[Columns.STEP.ordinal()].setValue(step);
                allFields[Columns.INFOSTATUS.ordinal()].setValue(infostatus.getCode());
                isSaved = false;
                this.setErrorExecutionStatus(infostatus);
                this.saveStatus();
                throw new OpenR66RunnerErrorException("Runner is in error: " +
                        e.getMessage(), e);
            }
            if ((!future.isDone()) || future.isFailed()) {
                R66Result result = future.getResult();
                if (result != null) {
                    infostatus = future.getResult().code;
                } else {
                    infostatus = ErrorCode.ExternalOp;
                }
                this.setErrorExecutionStatus(infostatus);
                allFields[Columns.STEP.ordinal()].setValue(step);
                allFields[Columns.INFOSTATUS.ordinal()].setValue(infostatus.getCode());
                isSaved = false;
                this.saveStatus();
                // FIXME
                logger.info("Future is failed: "+infostatus.mesg);
                if (future.getCause() != null) {
                    throw new OpenR66RunnerErrorException("Runner is failed: " +
                        future.getCause().getMessage(), future.getCause());
                } else {
                    throw new OpenR66RunnerErrorException("Runner is failed: " +
                            infostatus.mesg);
                }
            }
            step ++;
        }
    }

    /**
     * Once the transfer is over, finalize the Runner by running
     * the error or post operation according to the status.
     * @param localChannelReference
     * @param file
     * @param finalValue
     * @param status
     * @throws OpenR66RunnerErrorException
     * @throws OpenR66ProtocolSystemException
     */
    public void finalizeTransfer(LocalChannelReference localChannelReference, R66File file,
            R66Result finalValue, boolean status)
    throws OpenR66RunnerErrorException, OpenR66ProtocolSystemException {
        if (session == null) {
            this.session = localChannelReference.getSession();
        }
        if (status) {
            // First move the file
            if (this.isSender()) {
                // Nothing to do since it is the original file
                this.setPostTask();
            } else {
                int poststep = this.step;
                this.setPostTask();
                this.saveStatus();
                if (!RequestPacket.isRecvThroughMode(this.getMode())) {
                    if (this.globalstep == TASKSTEP.TRANSFERTASK.ordinal() ||
                            (this.globalstep == TASKSTEP.POSTTASK.ordinal() &&
                                    poststep == 0)) {
                        // Result file moves
                        String finalpath = R66Dir.getFinalUniqueFilename(file);
                        logger.debug("Will move file {}", finalpath);
                        try {
                            file.renameTo(this.getRule().setRecvPath(finalpath));
                        } catch (OpenR66ProtocolSystemException e) {
                            R66Result result = new R66Result(e, null, false,
                                    ErrorCode.FinalOp, this);
                            result.file = file;
                            result.runner = this;
                            if (localChannelReference != null) {
                                localChannelReference.invalidateRequest(result);
                            }
                            throw e;
                        } catch (CommandAbstractException e) {
                            R66Result result = new R66Result(
                                    new OpenR66RunnerErrorException(e), null,
                                    false, ErrorCode.FinalOp, this);
                            result.file = file;
                            result.runner = this;
                            if (localChannelReference != null) {
                                localChannelReference.invalidateRequest(result);
                            }
                            throw (OpenR66RunnerErrorException) result.exception;
                        }
                        logger.debug("File finally moved: {}", file);
                        try {
                            this.setFilename(file.getFile());
                        } catch (CommandAbstractException e) {
                        }
                    }
                }
            }
            this.saveStatus();
            if (RequestPacket.isSendThroughMode(this.getMode()) ||
                    RequestPacket.isRecvThroughMode(this.getMode())) {
                // File could not exist
            } else if (this.step == 0) {
                // File must exist
                try {
                    if (! file.exists()) {
                        // error
                        R66Result error =
                            new R66Result(this.session, finalValue.isAnswered,
                                    ErrorCode.FileNotFound, this);
                        this.setErrorExecutionStatus(ErrorCode.FileNotFound);
                        errorTransfer(error, file, localChannelReference);
                        return;
                    }
                } catch (CommandAbstractException e) {
                    // error
                    R66Result error =
                        new R66Result(this.session, finalValue.isAnswered,
                                ErrorCode.FileNotFound, this);
                    this.setErrorExecutionStatus(ErrorCode.FileNotFound);
                    errorTransfer(error, file, localChannelReference);
                    return;
                }
            }
            try {
                this.run();
            } catch (OpenR66RunnerErrorException e1) {
                R66Result result = new R66Result(e1, null, false,
                        ErrorCode.ExternalOp, this);
                result.file = file;
                result.runner = this;
                this.changeUpdatedInfo(UpdatedInfo.INERROR);
                this.saveStatus();
                if (localChannelReference != null) {
                    localChannelReference.invalidateRequest(result);
                }
                throw e1;
            }
            this.saveStatus();
            this.setAllDone();
            this.saveStatus();
            logger.info("Transfer done on {} at RANK {}",file != null ? file : "no file", rank);
            if (localChannelReference != null) {
                localChannelReference.validateEndTransfer(finalValue);
            }
        } else {
            if (!continueTransfer) {
                // already setup
                return;
            }
            errorTransfer(finalValue, file, localChannelReference);
        }
    }
    /**
     * Finalize a transfer in error
     * @param finalValue
     * @param file
     * @param localChannelReference
     * @throws OpenR66RunnerErrorException
     */
    private void errorTransfer(R66Result finalValue, R66File file,
            LocalChannelReference localChannelReference) throws OpenR66RunnerErrorException {
        // error or not ?
        ErrorCode runnerStatus = this.getErrorInfo();
        if (finalValue.exception != null) {
            logger.error("Transfer KO on " + file+ " due to "+ finalValue.exception.getMessage());
        } else {
            logger.error("Transfer KO on " + file+" due to "+finalValue.toString());
        }
        if (runnerStatus == ErrorCode.CanceledTransfer) {
            // delete file, reset runner
            this.setRankAtStartup(0);
            this.deleteTempFile();
            this.changeUpdatedInfo(UpdatedInfo.INERROR);
            this.saveStatus();
        } else if (runnerStatus == ErrorCode.StoppedTransfer) {
            // just save runner and stop
            this.changeUpdatedInfo(UpdatedInfo.INERROR);
            this.saveStatus();
        } else if (runnerStatus == ErrorCode.Shutdown) {
            // just save runner and stop
            this.changeUpdatedInfo(UpdatedInfo.INERROR);
            this.saveStatus();
        } else {
            if (this.globalstep != TASKSTEP.ERRORTASK.ordinal()) {
                // errorstep was not already executed
                // real error
                this.setErrorTask();
                this.saveStatus();
                try {
                    this.run();
                } catch (OpenR66RunnerErrorException e1) {
                    this.changeUpdatedInfo(UpdatedInfo.INERROR);
                    this.setErrorExecutionStatus(runnerStatus);
                    this.saveStatus();
                    if (localChannelReference != null) {
                        localChannelReference.invalidateRequest(finalValue);
                    }
                    throw e1;
                }
            }
            this.changeUpdatedInfo(UpdatedInfo.INERROR);
            if (RequestPacket.isRecvThroughMode(this.getMode()) ||
                    RequestPacket.isSendThroughMode(this.getMode())) {
                this.setErrorExecutionStatus(runnerStatus);
                this.saveStatus();
                if (localChannelReference != null) {
                    localChannelReference.invalidateRequest(finalValue);
                }
                return;
            }
        }
        // re set the original status
        this.setErrorExecutionStatus(runnerStatus);
        this.saveStatus();
        if (localChannelReference != null) {
            localChannelReference.invalidateRequest(finalValue);
        }
    }
    /**
     * Increment the rank of the transfer
     * @throws OpenR66ProtocolPacketException
     */
    public void incrementRank() throws OpenR66ProtocolPacketException {
        rank ++;
        allFields[Columns.RANK.ordinal()].setValue(rank);
        isSaved = false;
        if (rank % 10 == 0) {
            // Save each 10 blocks
            try {
                update();
            } catch (OpenR66DatabaseException e) {
                logger.warn("Cannot update Runner: {}", e.getMessage());
            }
        }
    }

    /**
     * This method is to be called each time an operation is happening on Runner
     *
     * @throws OpenR66RunnerErrorException
     */
    public void saveStatus() throws OpenR66RunnerErrorException {
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

    /**
     * Delete the temporary empty file (retrieved file at rank 0)
     */
    public void deleteTempFile() {
        if ((!isSender()) && getRank() == 0) {
            try {
                if (session != null) {
                    R66File file = session.getFile();
                    if (file != null) {
                        file.delete();
                    }
                }
            } catch (CommandAbstractException e1) {
                logger.warn("Cannot delete temporary empty file", e1);
            }
        }
    }

    @Override
    public String toString() {
        return "Run: " + (rule != null? rule.toString() : ruleId) + " on " +
                filename + " STEP: " + TASKSTEP.values()[globalstep] + "(" +
                TASKSTEP.values()[globallaststep] + "):" + step + ":" +
                status.mesg + " Transfer Rank: " + rank + " SpecialId: " +
                specialId + " isSender: " + isSender + " isMoved: " +
                isFileMoved + " Mode: " + TRANSFERMODE.values()[mode] +
                " Requester: " + requesterHostId + " Requested: " +
                requestedHostId + " Start: " + start + " Stop: " + stop +
                " Internal: " + UpdatedInfo.values()[updatedInfo].name()+
                ":"+infostatus.mesg+
                " Fileinfo: "+fileInformation;
    }

    public String toShortNoHtmlString(String newline) {
        return "Run: " + ruleId + " on " +
                filename + newline+" STEP: " + TASKSTEP.values()[globalstep] + "(" +
                TASKSTEP.values()[globallaststep] + "):" + step + ":" +
                status.mesg + newline+" Transfer Rank: " + rank + " SpecialId: " +
                specialId + " isSender: " + isSender + " isMoved: " +
                isFileMoved + " Mode: " + TRANSFERMODE.values()[mode] +
                newline+" Requester: " + requesterHostId + " Requested: " +
                requestedHostId + " Start: " + start + " Stop: " + stop +
                newline+" Internal: " + UpdatedInfo.values()[updatedInfo].name()+
                ":"+infostatus.mesg+
                newline+" Fileinfo: "+fileInformation;
    }

    public String toShortString() {
        return "<RULE>" + ruleId + "</RULE><ID>" + specialId + "</ID><FILE>" +
                filename + "</FILE>\n    <STEP>" + TASKSTEP.values()[globalstep] +
                "(" + TASKSTEP.values()[globallaststep] + "):" + step + ":" +
                status.mesg + "</STEP><RANK>" + rank + "</RANK>\n    <SENDER>" +
                isSender + "</SENDER><MOVED>" + isFileMoved + "</MOVED><MODE>" +
                TRANSFERMODE.values()[mode] + "</MODE>\n    <REQR>" +
                requesterHostId + "</REQR><REQD>" + requestedHostId +
                "</REQD>\n    <START>" + start + "</START><STOP>" + stop +
                "</STOP>\n    <INTERNAL>" + UpdatedInfo.values()[updatedInfo].name()
                +" : "+infostatus.mesg+ "</INTERNAL>\n    <FILEINFO>"+
                fileInformation+"</FILEINFO>";
    }

    /**
     *
     * @return the header for a table of runners in Html format
     */
    public static String headerHtml() {
        return "<td>SpecialId</td><td>Rule</td><td>Filename</td><td>Info"
                + "</td><td>Step (LastStep)</td><td>Action</td><td>Status"
                + "</td><td>Internal</t><td>Transfer Rank</td><td>isMoved"
                + "</td><td>Requester</td><td>Requested"
                + "</td><td>Start</td><td>Stop</td><td>Bandwidth (Mbits)</td><td>Free Space(MB)</td>";
    }

    /**
     * @param session
     * @return The associated freespace of the current directory
     */
    public long freespace(R66Session session) {
        long freespace = -1;
        DbRule rule = null;
        try {
            rule = (this.rule != null)? this.rule : new DbRule(this.dbSession,
                    this.ruleId);
        } catch (OpenR66DatabaseException e) {
        }
        if (this.rule == null) {
            this.rule = rule;
        }
        if (rule != null) {
            if (!this.isSender) {
                try {
                    String sdir;
                    if (this.globallaststep == TASKSTEP.ALLDONETASK.ordinal()) {
                        // all finished
                        sdir = rule.recvPath;
                    } else if (this.globallaststep == TASKSTEP.POSTTASK
                            .ordinal()) {
                        // Post task
                        sdir = rule.recvPath;
                    } else {
                        // are we in sending or receive
                        sdir = rule.workPath;
                    }
                    R66Dir dir;
                    if (HttpFormattedHandler.usedDir.containsKey(sdir)) {
                        dir = HttpFormattedHandler.usedDir.get(sdir);
                    } else {
                        dir = new R66Dir(session);
                        dir.changeDirectory(sdir);
                        HttpFormattedHandler.usedDir.put(sdir, dir);
                    }
                    freespace = dir.getFreeSpace() / 0x100000L;
                } catch (CommandAbstractException e) {
                    logger.warn("Error while freespace compute {}", e.getMessage());
                }
            }
        }
        return freespace;
    }

    private String bandwidth() {
        double drank = (rank<=0 ? 1 : rank);
        double dblocksize = blocksize*8;
        double size = drank*dblocksize;
        double time = (stop.getTime() + 1 - start.getTime());
        double result = size/time / ((double) 0x100000L) * ((double) 1000);
        return String.format("%,.2f", result);
    }

    private String getHtmlColor() {
        String color;
        switch (TASKSTEP.values()[globalstep]) {
            case NOTASK:
                color = "Orange";
                break;
            case PRETASK:
                color = "Yellow";
                break;
            case TRANSFERTASK:
                color = "LightGreen";
                break;
            case POSTTASK:
                color = "Turquoise";
                break;
            case ERRORTASK:
                color = "Red";
                break;
            case ALLDONETASK:
                color = "Cyan";
                break;
            default:
                color = "";
        }
        return color;
    }

    private String getInfoHtmlColor() {
        String color;
        switch (UpdatedInfo.values()[updatedInfo]) {
            case DONE:
                color = "Cyan";
                break;
            case INERROR:
                color = "Red";
                break;
            case INTERRUPTED:
                color = "Orange";
                break;
            case NOTUPDATED:
                color = "Yellow";
                break;
            case RUNNING:
                color = "LightGreen";
                break;
            case TOSUBMIT:
                color = "Turquoise";
                break;
            case UNKNOWN:
                color = "Turquoise";
                break;
            default:
                color = "";
        }
        return color;
    }

    /**
     * @param session
     * @param running special info
     * @return the runner in Html format compatible with the header from
     *         headerHtml method
     */
    public String toHtml(R66Session session, String running) {
        long freespace = freespace(session);
        String color = getHtmlColor();
        String updcolor = getInfoHtmlColor();
        return "<td>" +
                specialId +
                "</td><td>" +
                (rule != null? rule.toShortString() : ruleId) +
                "</td><td>" +
                filename +
                "</td><td>" +fileInformation+
                "</td><td bgcolor=\"" +
                color +
                "\">" +
                TASKSTEP.values()[globalstep] +
                " (" +
                TASKSTEP.values()[globallaststep] +
                ")</td><td>" +
                step +
                "</td><td>" +
                status.mesg+" <b>"+running+
                "</b></td><td bgcolor=\"" +
                updcolor+"\">" +
                UpdatedInfo.values()[updatedInfo].name()+" : "+infostatus.mesg +
                "</td><td>" +
                rank +
                "</td><td>" +
                isFileMoved +
                "</td><td>" +
                requesterHostId +
                "</td><td>" +
                requestedHostId +
                "</td><td>" +
                start +
                "</td><td>" +
                stop +
                "</td><td>" +
                bandwidth() + "</td>" + "<td>" +
                freespace + "</td>";
    }

    /**
     * @param session
     * @param body
     * @param running special info
     * @return the runner in Html format specified by body by replacing all
     *         instance of fields
     */
    public String toSpecializedHtml(R66Session session, String body, String running) {
        long freespace = freespace(session);
        StringBuilder builder = new StringBuilder(body);
        FileUtils.replaceAll(builder, "XXXSpecIdXXX", Long.toString(specialId));
        FileUtils.replace(builder, "XXXRulXXX", (rule != null? rule.toShortString()
                : ruleId));
        FileUtils.replace(builder, "XXXFileXXX", filename);
        FileUtils.replace(builder, "XXXInfoXXX", fileInformation);
        FileUtils.replace(builder, "XXXStepXXX", TASKSTEP.values()[globalstep] + " (" +
                TASKSTEP.values()[globallaststep] + ")");
        FileUtils.replace(builder, "XXXCOLXXX", getHtmlColor());
        FileUtils.replace(builder, "XXXActXXX", Integer.toString(step));
        FileUtils.replace(builder, "XXXStatXXX", status.mesg);
        FileUtils.replace(builder, "XXXRunningXXX", running);
        FileUtils.replace(builder, "XXXInternXXX", UpdatedInfo.values()[updatedInfo].name()+
                " : "+infostatus.mesg);
        FileUtils.replace(builder, "XXXUPDCOLXXX", getInfoHtmlColor());
        FileUtils.replace(builder, "XXXBloXXX", Integer.toString(rank));
        FileUtils.replace(builder, "XXXisSendXXX", Boolean.toString(isSender));
        FileUtils.replace(builder, "XXXisMovXXX", Boolean.toString(isFileMoved));
        FileUtils.replace(builder, "XXXModXXX", TRANSFERMODE.values()[mode].toString());
        FileUtils.replaceAll(builder, "XXXReqrXXX", requesterHostId);
        FileUtils.replaceAll(builder, "XXXReqdXXX", requestedHostId);
        FileUtils.replace(builder, "XXXStarXXX", start.toString());
        FileUtils.replace(builder, "XXXStopXXX", stop.toString());
        FileUtils.replace(builder, "XXXBandXXX", bandwidth());
        FileUtils.replace(builder, "XXXFreeXXX", Long.toString(freespace));
        return builder.toString();
    }

    /**
     *
     * @return True if the current host is the requested host (to prevent
     *         request to itself)
     */
    public boolean isSelfRequested() {
        return (this.requestedHostId
                .equals(Configuration.configuration.HOST_ID) || this.requestedHostId
                .equals(Configuration.configuration.HOST_SSLID));
    }

    /**
     *
     * @return the requested HostId
     */
    public String getRequested() {
        return this.requestedHostId;
    }

    /**
     *
     * @return the requester HostId
     */
    public String getRequester() {
        return this.requesterHostId;
    }

    /**
     * @return the start
     */
    public Timestamp getStart() {
        return start;
    }

    /**
     * @return the stop
     */
    public Timestamp getStop() {
        return stop;
    }

    /**
     *
     * @return the associated request
     */
    public RequestPacket getRequest() {
        return new RequestPacket(ruleId, mode, originalFilename, blocksize,
                rank, specialId, fileInformation);
    }

    /**
     * Used internally
     *
     * @return a Key representing the primary key as a unique string
     */
    public String getKey() {
        return requestedHostId + " " + requesterHostId + " " + specialId;
    }

    /**
     * Construct a new Element with value
     *
     * @param name
     * @param value
     * @return the new Element
     */
    private static Element newElement(String name, String value) {
        Element node = new DefaultElement(name);
        if (value != null) {
            node.addText(value);
        }
        return node;
    }

    /**
     * Need to call 'setToArray' before
     * @param runner
     * @return The Element representing the given Runner
     * @throws OpenR66DatabaseSqlError
     */
    private static Element getElementFromRunner(DbTaskRunner runner)
            throws OpenR66DatabaseSqlError {
        Element root = new DefaultElement(XMLRUNNER);
        for (DbValue value: runner.allFields) {
            if (value.column.equals(Columns.UPDATEDINFO.name())) {
                continue;
            }
            root.add(newElement(value.column.toLowerCase(), value
                    .getValueAsString()));
        }
        return root;
    }
    /**
     * Set the given runner from the root element of the runner itself (XMLRUNNER but not XMLRUNNERS).
     * Need to call 'setFromArray' after.
     * @param runner
     * @param root
     * @throws OpenR66DatabaseSqlError
     */
    private static void setRunnerFromElement(DbTaskRunner runner, Element root) 
        throws OpenR66DatabaseSqlError {
        for (DbValue value: runner.allFields) {
            if (value.column.equals(Columns.UPDATEDINFO.name())) {
                continue;
            }
            Element elt = (Element) root.selectSingleNode(value.column.toLowerCase());
            if (elt != null) {
                String newValue = elt.getText();
                value.setValueFromString(newValue);
            }
        }
    }
    /**
     * Write the selected TaskRunners from PrepareStatement to a XMLWriter
     *
     * @param preparedStatement
     *            ready to be executed
     * @param xmlWriter
     * @return the NbAndSpecialId for the number of transfer and higher rank found
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     * @throws OpenR66ProtocolBusinessException
     */
    public static NbAndSpecialId writeXML(DbPreparedStatement preparedStatement, XMLWriter xmlWriter)
            throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError, OpenR66ProtocolBusinessException {
        Element root = new DefaultElement(XMLRUNNERS);
        NbAndSpecialId nbAndSpecialId = new NbAndSpecialId();
        try {
            xmlWriter.writeOpen(root);
            Element node;
            while (preparedStatement.getNext()) {
                DbTaskRunner runner = DbTaskRunner
                        .getFromStatement(preparedStatement);
                if (nbAndSpecialId.higherSpecialId < runner.specialId) {
                    nbAndSpecialId.higherSpecialId = runner.specialId;
                }
                node = DbTaskRunner.getElementFromRunner(runner);
                xmlWriter.write(node);
                xmlWriter.flush();
                nbAndSpecialId.nb ++;
            }
            xmlWriter.writeClose(root);
        } catch (IOException e) {
            logger.error("Cannot write XML file", e);
            throw new OpenR66ProtocolBusinessException("Cannot write file: "+e.getMessage());
        }
        return nbAndSpecialId;
    }
    /**
     * Write selected TaskRunners to an XML file using an XMLWriter
     *
     * @param preparedStatement
     * @param filename
     * @return the NbAndSpecialId for the number of transfer and higher rank found
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     * @throws OpenR66ProtocolBusinessException
     */
    public static NbAndSpecialId writeXMLWriter(DbPreparedStatement preparedStatement, String filename)
            throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError,
            OpenR66ProtocolBusinessException {
        NbAndSpecialId nbAndSpecialId = null;
        OutputStream outputStream = null;
        XMLWriter xmlWriter = null;
        try {
            outputStream = new FileOutputStream(filename);
            OutputFormat format = OutputFormat.createPrettyPrint();
            format.setEncoding("ISO-8859-1");
            xmlWriter = new XMLWriter(outputStream, format);
            preparedStatement.executeQuery();
            nbAndSpecialId = writeXML(preparedStatement, xmlWriter);
        } catch (FileNotFoundException e) {
            logger.error("Cannot write XML file", e);
            throw new OpenR66ProtocolBusinessException("File not found");
        } catch (UnsupportedEncodingException e) {
            logger.error("Cannot write XML file", e);
            throw new OpenR66ProtocolBusinessException("Unsupported Encoding");
        } finally {
            if (xmlWriter != null) {
                try {
                    xmlWriter.endDocument();
                    xmlWriter.flush();
                    xmlWriter.close();
                } catch (SAXException e) {
                    try {
                        outputStream.close();
                    } catch (IOException e2) {
                    }
                    File file = new File(filename);
                    file.delete();
                    logger.error("Cannot write XML file", e);
                    throw new OpenR66ProtocolBusinessException("Unsupported Encoding");
                } catch (IOException e) {
                    try {
                        outputStream.close();
                    } catch (IOException e2) {
                    }
                    File file = new File(filename);
                    file.delete();
                    logger.error("Cannot write XML file", e);
                    throw new OpenR66ProtocolBusinessException("Unsupported Encoding");
                }
            } else if (outputStream != null){
                try {
                    outputStream.close();
                } catch (IOException e) {
                }
                File file = new File(filename);
                file.delete();
            }
        }
        return nbAndSpecialId;
    }

    /**
     * Write all TaskRunners to an XML file using an XMLWriter
     *
     * @param filename
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     * @throws OpenR66ProtocolBusinessException
     */
    public static void writeXMLWriter(String filename)
            throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError,
            OpenR66ProtocolBusinessException {
        String request = "SELECT " + DbTaskRunner.selectAllFields + " FROM " +
                DbTaskRunner.table+" WHERE "+getLimitWhereCondition();
        DbPreparedStatement preparedStatement = null;
        try {
            preparedStatement = new DbPreparedStatement(
                    DbConstant.admin.session);
            preparedStatement.createPrepareStatement(request);
            writeXMLWriter(preparedStatement, filename);
        } finally {
            if (preparedStatement != null) {
                preparedStatement.realClose();
            }
        }
    }
    /**
     * 
     * @return the backend XML filename for the current TaskRunner in NoDb Client mode
     */
    public String backendXmlFilename() {
        return Configuration.configuration.baseDirectory+
            Configuration.configuration.archivePath+R66Dir.SEPARATOR+
            this.requesterHostId+"_"+this.requestedHostId+"_"+this.ruleId+"_"+this.specialId
            +"_singlerunner.xml";
    }
    /**
     * Method to write the current DbTaskRunner for NoDb client instead of updating DB.
     * 'setToArray' must be called priorly to be able to store the values.
     * @throws OpenR66ProtocolBusinessException
     */
    public void writeXmlWorkNoDb() throws OpenR66ProtocolBusinessException {
        String filename = backendXmlFilename();
        OutputStream outputStream = null;
        XMLWriter xmlWriter = null;
        try {
            outputStream = new FileOutputStream(filename);
            OutputFormat format = OutputFormat.createPrettyPrint();
            format.setEncoding("ISO-8859-1");
            xmlWriter = new XMLWriter(outputStream, format);
            Element root = new DefaultElement(XMLRUNNERS);
            try {
                xmlWriter.writeOpen(root);
                Element node;
                node = DbTaskRunner.getElementFromRunner(this);
                xmlWriter.write(node);
                xmlWriter.flush();
                xmlWriter.writeClose(root);
            } catch (IOException e) {
                logger.error("Cannot write XML file", e);
                throw new OpenR66ProtocolBusinessException("Cannot write file: "+e.getMessage());
            } catch (OpenR66DatabaseSqlError e) {
                logger.error("Cannot write Data", e);
                throw new OpenR66ProtocolBusinessException("Cannot write Data: "+e.getMessage());
            }
        } catch (FileNotFoundException e) {
            logger.error("Cannot write XML file", e);
            throw new OpenR66ProtocolBusinessException("File not found");
        } catch (UnsupportedEncodingException e) {
            logger.error("Cannot write XML file", e);
            throw new OpenR66ProtocolBusinessException("Unsupported Encoding");
        } finally {
            if (xmlWriter != null) {
                try {
                    xmlWriter.endDocument();
                    xmlWriter.flush();
                    xmlWriter.close();
                } catch (SAXException e) {
                    try {
                        outputStream.close();
                    } catch (IOException e2) {
                    }
                    File file = new File(filename);
                    file.delete();
                    logger.error("Cannot write XML file", e);
                    throw new OpenR66ProtocolBusinessException("Unsupported Encoding");
                } catch (IOException e) {
                    try {
                        outputStream.close();
                    } catch (IOException e2) {
                    }
                    File file = new File(filename);
                    file.delete();
                    logger.error("Cannot write XML file", e);
                    throw new OpenR66ProtocolBusinessException("Unsupported Encoding");
                }
            } else if (outputStream != null){
                try {
                    outputStream.close();
                } catch (IOException e) {
                }
                File file = new File(filename);
                file.delete();
            }
        }
    }
    /**
     * Method to load a previous existing DbTaskRunner for NoDb client from File instead of from DB.
     * 'setFromArray' must be called after.
     * @throws OpenR66ProtocolBusinessException
     */
    public void loadXmlWorkNoDb() throws OpenR66ProtocolBusinessException {
        String filename = backendXmlFilename();
        File file = new File(filename);
        if (! file.canRead()) {
            throw new OpenR66ProtocolBusinessException("Backend XML file cannot be read");
        }
        SAXReader reader = new SAXReader();
        Document document;
        try {
            document = reader.read(file);
        } catch (DocumentException e) {
            throw new OpenR66ProtocolBusinessException("Backend XML file cannot be read as an XML file");
        }
        Element root = (Element) document.selectSingleNode("/"+XMLRUNNERS+"/"+XMLRUNNER);
        try {
            setRunnerFromElement(this, root);
        } catch (OpenR66DatabaseSqlError e) {
            throw new OpenR66ProtocolBusinessException("Backend XML file is not conform to the model");
        }
    }
    /**
     * 
     * @return True if the backend XML for NoDb client is available for this TaskRunner
     */
    public boolean existXmlWorkNoDb() {
        String filename = backendXmlFilename();
        File file = new File(filename);
        return file.canRead();
    }
    /**
     * Delete the backend XML file for the current TaskRunner for NoDb Client
     */
    public void deleteXmlWorkNoDb() {
        File file = new File(backendXmlFilename());
        file.delete();
    }
}
