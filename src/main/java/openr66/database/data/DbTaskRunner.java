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

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.tree.DefaultElement;

import openr66.context.ErrorCode;
import openr66.context.R66Session;
import openr66.context.filesystem.R66Dir;
import openr66.context.filesystem.R66File;
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
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNoSslException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.http.HttpHandler;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.localhandler.packet.RequestPacket.TRANSFERMODE;
import openr66.protocol.utils.FileUtils;
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
        START,
        STOP,
        UPDATEDINFO,
        REQUESTER,
        REQUESTED,
        SPECIALID;
    }

    public static int[] dbTypes = {
            Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER,
            Types.CHAR, Types.BIT, Types.VARCHAR, Types.BIT, Types.VARCHAR,
            Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
            Types.TIMESTAMP, Types.TIMESTAMP, Types.INTEGER,
            Types.VARCHAR, Types.VARCHAR, Types.BIGINT };

    public static String table = " RUNNER ";

    public static String fieldseq = "RUNSEQ";

    /**
     * GlobalStep Value
     */
    public static enum TASKSTEP {
        NOTASK, PRETASK, TRANSFERTASK, POSTTASK, ALLDONETASK, ERRORTASK;
    }

    // Values
    private DbRule rule;

    private final R66Session session;

    private int globalstep = TASKSTEP.NOTASK.ordinal();

    private int globallaststep = TASKSTEP.NOTASK.ordinal();

    private int step = -1;

    private int rank = 0;

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

    private String requesterHostId;

    private String requestedHostId;

    private Timestamp start;

    private Timestamp stop;

    private int updatedInfo = UpdatedInfo.UNKNOWN.ordinal();

    private boolean isSaved = false;

    // ALL TABLE SHOULD IMPLEMENT THIS
    private final DbValue primaryKey[] = {
            new DbValue(requesterHostId, Columns.REQUESTER.name()),
            new DbValue(requestedHostId, Columns.REQUESTED.name()),
            new DbValue(specialId, Columns.SPECIALID.name()) };

    private final DbValue[] otherFields = {
            // GLOBALSTEP, GLOBALLASTSTEP, STEP, RANK, STEPSTATUS, RETRIEVEMODE,
            // FILENAME, ISMOVED, IDRULE,
            // BLOCKSIZE, ORIGINALNAME, FILEINFO, MODE,
            // START, STOP
            // UPDATEDINFO
            new DbValue(globalstep, Columns.GLOBALSTEP.name()),
            new DbValue(globallaststep, Columns.GLOBALLASTSTEP.name()),
            new DbValue(step, Columns.STEP.name()),
            new DbValue(rank, Columns.RANK.name()),
            new DbValue(status.getCode(), Columns.STEPSTATUS.name()),
            new DbValue(isSender, Columns.RETRIEVEMODE.name()),
            new DbValue(filename, Columns.FILENAME.name()),
            new DbValue(isFileMoved, Columns.ISMOVED.name()),
            new DbValue(ruleId, Columns.IDRULE.name()),
            new DbValue(blocksize, Columns.BLOCKSIZE.name()),
            new DbValue(originalFilename, Columns.ORIGINALNAME.name()),
            new DbValue(fileInformation, Columns.FILEINFO.name()),
            new DbValue(mode, Columns.MODE.name()),
            new DbValue(start, Columns.START.name()),
            new DbValue(stop, Columns.STOP.name()),
            new DbValue(updatedInfo, Columns.UPDATEDINFO.name()) };

    private final DbValue[] allFields = {
            otherFields[0], otherFields[1], otherFields[2], otherFields[3],
            otherFields[4], otherFields[5], otherFields[6], otherFields[7],
            otherFields[8], otherFields[9], otherFields[10], otherFields[11],
            otherFields[12], otherFields[13], otherFields[14], otherFields[15],
            primaryKey[0], primaryKey[1], primaryKey[2] };

    public static final String selectAllFields = Columns.GLOBALSTEP.name() +
            "," + Columns.GLOBALLASTSTEP.name() + "," + Columns.STEP.name() +
            "," + Columns.RANK.name() + "," + Columns.STEPSTATUS.name() + "," +
            Columns.RETRIEVEMODE.name() + "," + Columns.FILENAME.name() + "," +
            Columns.ISMOVED.name() + "," + Columns.IDRULE.name() + "," +
            Columns.BLOCKSIZE.name() + "," + Columns.ORIGINALNAME.name() + "," +
            Columns.FILEINFO.name() + "," + Columns.MODE.name() + "," +
            Columns.START.name() + "," + Columns.STOP.name() + "," +
            Columns.UPDATEDINFO.name() + "," +
            Columns.REQUESTER.name() + "," + Columns.REQUESTED.name() + "," +
            Columns.SPECIALID.name();

    private static final String updateAllFields = Columns.GLOBALSTEP.name() +
            "=?," + Columns.GLOBALLASTSTEP.name() + "=?," +
            Columns.STEP.name() + "=?," + Columns.RANK.name() + "=?," +
            Columns.STEPSTATUS.name() + "=?," + Columns.RETRIEVEMODE.name() +
            "=?," + Columns.FILENAME.name() + "=?," + Columns.ISMOVED.name() +
            "=?," + Columns.IDRULE.name() + "=?," + Columns.BLOCKSIZE.name() +
            "=?," + Columns.ORIGINALNAME.name() + "=?," +
            Columns.FILEINFO.name() + "=?," + Columns.MODE.name() + "=?," +
            Columns.START.name() + "=?," + Columns.STOP.name() + "=?," +
            Columns.UPDATEDINFO.name() + "=?";

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
            try {
                return Configuration.configuration.getHostId(
                        session.getAuth().isSsl());
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
                return Configuration.configuration.getHostId(
                        session.getAuth().isSsl());
            } catch (OpenR66ProtocolNoSslException e) {
                return Configuration.configuration.HOST_ID;
            }
        }
    }

    /**
     * Constructor for submission (no transfer session), from database it is created, so with a new
     * specialId if necessary
     *
     * @param dbSession
     * @param rule
     * @param isSender
     * @param requestPacket
     * @param requested
     * @throws OpenR66DatabaseException
     */
   public DbTaskRunner(DbSession dbSession, DbRule rule,
           boolean isSender, RequestPacket requestPacket, String requested) throws OpenR66DatabaseException {
       super(dbSession);
       this.session = null;
       this.rule = rule;
       ruleId = this.rule.idRule;
       rank = requestPacket.getRank();
       status = ErrorCode.Unknown;
       this.isSender = isSender;
       filename = requestPacket.getFilename();
       blocksize = requestPacket.getBlocksize();
       originalFilename = requestPacket.getFilename();
       fileInformation = requestPacket.getFileInformation();
       mode = requestPacket.getMode();
       //itself but according to SSL
       requesterHostId = Configuration.configuration.getHostId(dbSession,requested);
       //given one
       requestedHostId = requested;

       start = new Timestamp(System.currentTimeMillis());
       setToArray();
       isSaved = false;
       specialId = requestPacket.getSpecialId();
       create();
   }
    /**
     * Constructor from a request with a valid Special Id
     * @param dbSession
     * @param session
     * @param rule
     * @param isSender
     * @param requestPacket
     * @throws OpenR66DatabaseException
     */
    public DbTaskRunner(DbSession dbSession, R66Session session, DbRule rule,
            boolean isSender, RequestPacket requestPacket) throws OpenR66DatabaseException {
        super(dbSession);
        this.session = session;
        this.rule = rule;
        ruleId = this.rule.idRule;
        rank = requestPacket.getRank();
        status = ErrorCode.Unknown;
        this.isSender = isSender;
        filename = requestPacket.getFilename();
        blocksize = requestPacket.getBlocksize();
        originalFilename = requestPacket.getFilename();
        fileInformation = requestPacket.getFileInformation();
        mode = requestPacket.getMode();
        requesterHostId = getRequester(session, requestPacket);
        requestedHostId = getRequested(session, requestPacket);

        start = new Timestamp(System.currentTimeMillis());
        setToArray();
        isSaved = false;
        specialId = requestPacket.getSpecialId();
        insert();
    }
    /**
     * Constructor from a request with a valid Special Id
     * @param dbSession
     * @param session
     * @param rule
     * @param id
     * @param requester
     * @param requested
     * @throws OpenR66DatabaseException
     */
    public DbTaskRunner(DbSession dbSession, R66Session session, DbRule rule, long id,
            String requester, String requested) throws OpenR66DatabaseException {
        super(dbSession);
        this.session = session;
        this.rule = rule;

        specialId = id;
        // retrieving a task should be made from the requester, but the caller
        // is responsible of this
        requestedHostId = requested;
        requesterHostId = requester;

        select();
        if (rule != null) {
            if (!ruleId.equals(rule.idRule)) {
                throw new OpenR66DatabaseNoDataException("Rule does not correspond");
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
        allFields[Columns.BLOCKSIZE.ordinal()].setValue(blocksize);
        allFields[Columns.ORIGINALNAME.ordinal()].setValue(originalFilename);
        allFields[Columns.FILEINFO.ordinal()].setValue(fileInformation);
        allFields[Columns.MODE.ordinal()].setValue(mode);
        allFields[Columns.START.ordinal()].setValue(start);
        stop = new Timestamp(System.currentTimeMillis());
        allFields[Columns.STOP.ordinal()].setValue(stop);
        allFields[Columns.UPDATEDINFO.ordinal()].setValue(updatedInfo);
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
        blocksize = (Integer) allFields[Columns.BLOCKSIZE.ordinal()].getValue();
        originalFilename = (String) allFields[Columns.ORIGINALNAME.ordinal()]
                .getValue();
        fileInformation = (String) allFields[Columns.FILEINFO.ordinal()]
                .getValue();
        mode = (Integer) allFields[Columns.MODE.ordinal()].getValue();
        start = (Timestamp) allFields[Columns.START.ordinal()].getValue();
        stop = (Timestamp) allFields[Columns.STOP.ordinal()].getValue();
        updatedInfo = (Integer) allFields[Columns.UPDATEDINFO.ordinal()]
                .getValue();
        requesterHostId = (String) allFields[Columns.REQUESTER.ordinal()]
                                             .getValue();
        requestedHostId = (String) allFields[Columns.REQUESTED.ordinal()]
                .getValue();
        specialId = (Long) allFields[Columns.SPECIALID.ordinal()].getValue();
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.data.AbstractDbData#delete()
     */
    @Override
    public void delete() throws OpenR66DatabaseException {
        if (dbSession == null) {
            return;
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("DELETE FROM " + table +
                    " WHERE " + primaryKey[0].column + " = ? AND " +
                    primaryKey[1].column + " = ? AND " +
                    primaryKey[2].column + " = ? ");
            primaryKey[0].setValue(requesterHostId);
            primaryKey[1].setValue(requestedHostId);
            primaryKey[2].setValue(specialId);
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
        if (dbSession == null) {
            if (specialId == DbConstant.ILLEGALVALUE) {
                logger.info("New SpecialId is not possible with No Database Model");
                specialId = System.currentTimeMillis();
            }
            isSaved = true;
            return;
        }
        // First need to find a new id if id is not ok
        if (specialId == DbConstant.ILLEGALVALUE) {
            specialId = DbModelFactory.dbModel.nextSequence(dbSession);
            logger.debug("Try Insert create a new Id from sequence: "+specialId);
            primaryKey[0].setValue(requesterHostId);
            primaryKey[1].setValue(requestedHostId);
            primaryKey[2].setValue(specialId);
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
     * @throws OpenR66DatabaseException
     */
    public void create() throws OpenR66DatabaseException {
        if (isSaved) {
            return;
        }
        if (dbSession == null) {
            if (specialId == DbConstant.ILLEGALVALUE) {
                logger.info("New SpecialId is not possible with No Database Model");
                specialId = System.currentTimeMillis();
            }
            isSaved = true;
            return;
        }
        // First need to find a new id if id is not ok
        if (specialId == DbConstant.ILLEGALVALUE) {
            specialId = DbModelFactory.dbModel.nextSequence(dbSession);
            logger.info("Try Insert create a new Id from sequence: "+specialId);
            primaryKey[0].setValue(requesterHostId);
            primaryKey[1].setValue(requestedHostId);
            primaryKey[2].setValue(specialId);
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
                logger.info("Problem while inserting",e);
                DbPreparedStatement find = new DbPreparedStatement(dbSession);
                find.createPrepareStatement("SELECT MAX("+primaryKey[2].column+
                        ") FROM "+table+" WHERE "+
                        primaryKey[0].column + " = ? AND " +
                        primaryKey[1].column + " = ? AND " +
                        primaryKey[2].column + " >= ? ");
                primaryKey[0].setValue(requesterHostId);
                primaryKey[1].setValue(requestedHostId);
                primaryKey[2].setValue(specialId);
                setValues(find, primaryKey);
                find.executeQuery();
                if (find.getNext()) {
                    long result;
                    try {
                        result = find.getResultSet().getLong(1);
                    } catch (SQLException e1) {
                        throw new OpenR66DatabaseSqlError(e1);
                    }
                    specialId = result+1;
                    DbModelFactory.dbModel.resetSequence(specialId+1);
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
            }
            isSaved = true;
        } finally {
            preparedStatement.realClose();
        }
    }

    /* (non-Javadoc)
     * @see openr66.database.data.AbstractDbData#exist()
     */
    @Override
    public boolean exist() throws OpenR66DatabaseException {
        if (dbSession == null) {
            return false;
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("SELECT " +
                    primaryKey[0].column + " FROM " + table + " WHERE " +
                    primaryKey[0].column + " = ? AND " +
                    primaryKey[1].column + " = ? AND " +
                    primaryKey[2].column + " = ? ");
            primaryKey[0].setValue(requesterHostId);
            primaryKey[1].setValue(requestedHostId);
            primaryKey[2].setValue(specialId);
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
     * @see openr66.database.data.AbstractDbData#select()
     */
    @Override
    public void select() throws OpenR66DatabaseException {
        if (dbSession == null) {
            throw new OpenR66DatabaseNoDataException("No row found");
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        preparedStatement.createPrepareStatement("SELECT " + selectAllFields +
                " FROM " + table + " WHERE " + primaryKey[0].column + " = ? AND " +
                primaryKey[1].column + " = ? AND " +
                primaryKey[2].column + " = ? ");
        try {
            primaryKey[0].setValue(requesterHostId);
            primaryKey[1].setValue(requestedHostId);
            primaryKey[2].setValue(specialId);
            setValues(preparedStatement, primaryKey);
            preparedStatement.executeQuery();
            if (preparedStatement.getNext()) {
                getValues(preparedStatement, allFields);
                setFromArray();
                isSaved = true;
            } else {
                throw new OpenR66DatabaseNoDataException("No row found: "+
                        primaryKey[0].getValueAsString()+":"+
                        primaryKey[1].getValueAsString()+":"+
                        primaryKey[2].getValueAsString());
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
        if (dbSession == null) {
            isSaved = true;
            return;
        }
        setToArray();
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("UPDATE " + table +
                    " SET " + updateAllFields + " WHERE " +
                    primaryKey[0].column + " = ? AND " +
                    primaryKey[1].column + " = ? AND " +
                    primaryKey[2].column + " = ? ");
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
     * @param session
     */
    private DbTaskRunner(DbSession dBsession) {
        super(dBsession);
        session = null;
        rule = null;
    }
    /**
     * For instance from Commander when getting updated information
     * @param preparedStatement
     * @return the next updated DbTaskRunner
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public static DbTaskRunner getFromStatement(DbPreparedStatement preparedStatement) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        DbTaskRunner dbTaskRunner = new DbTaskRunner(preparedStatement.getDbSession());
        dbTaskRunner.getValues(preparedStatement, dbTaskRunner.allFields);
        dbTaskRunner.setFromArray();
        dbTaskRunner.isSaved = true;
        return dbTaskRunner;
    }
    /**
    * @param session
    * @param status
    * @return the DbPreparedStatement for getting Runner according to status
    * @throws OpenR66DatabaseNoConnectionError
    * @throws OpenR66DatabaseSqlError
    */
   public static DbPreparedStatement getStatusPrepareStament(DbSession session,
           ErrorCode status) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
       String request = "SELECT " +selectAllFields+" FROM "+table;
       if (status != null) {
           request += " WHERE "+Columns.STEPSTATUS.name()+" = '"+status.getCode()+"'";
       }
       request += " ORDER BY "+Columns.START.name()+" DESC ";
       return new DbPreparedStatement(session, request);
   }
   /**
    * @param session
    * @param globalstep
    * @return the DbPreparedStatement for getting Runner according to globalstep
    * @throws OpenR66DatabaseNoConnectionError
    * @throws OpenR66DatabaseSqlError
    */
   public static DbPreparedStatement getStepPrepareStament(DbSession session,
           TASKSTEP globalstep) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
       String request = "SELECT " +selectAllFields+" FROM "+table;
       if (globalstep != null) {
           request += " WHERE "+Columns.GLOBALSTEP.name()+" = "+globalstep.ordinal();
       }
       request += " ORDER BY "+Columns.START.name()+" DESC ";
       return new DbPreparedStatement(session, request);
   }
   /**
   *
   * @param session
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
  public static DbPreparedStatement getFilterPrepareStament(DbSession session,
          Timestamp start, Timestamp stop, String rule, String req,
          boolean pending, boolean transfer, boolean error, boolean done, boolean all)
      throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
      DbPreparedStatement preparedStatement = new DbPreparedStatement(session);
      String request = "SELECT " +selectAllFields+" FROM "+table;
      if (start == null && stop == null && rule == null && req == null && all) {
          // finish
          request += " ORDER BY "+Columns.START.name();
          preparedStatement.createPrepareStatement(request);
          return preparedStatement;
      }
      request += " WHERE ";
      String condition = null;
      if (start != null & stop != null) {
          condition = Columns.START.name()+" >= ? AND "+
              Columns.START.name()+" <= ? ";
      } else if (start != null) {
          condition = Columns.START.name()+" >= ? ";
      } else if (stop != null) {
          condition = Columns.START.name()+" <= ? ";
      }

      if (rule != null) {
          if (condition != null) {
              condition += " AND ";
          } else {
              condition = "";
          }
          condition += Columns.IDRULE.name()+" LIKE '%"+rule+"%' ";
      }
      if (req != null) {
          if (condition != null) {
              condition += " AND ";
          } else {
              condition = "";
          }
          condition += "( "+Columns.REQUESTED.name()+" LIKE '%"+req+"%' OR "+
              Columns.REQUESTER.name()+" LIKE '%"+req+"%' )";
      }
      if (! all) {
          if (pending) {
              if (condition != null) {
                  condition += " AND ";
              } else {
                  condition = "";
              }
              condition += Columns.GLOBALSTEP.name()+" = "+TASKSTEP.NOTASK.ordinal();
          }
          if (transfer) {
              if (condition != null) {
                  condition += " AND ";
              } else {
                  condition = "";
              }
              condition += "( "+Columns.GLOBALSTEP.name()+" = "+TASKSTEP.PRETASK.ordinal();
              condition += " OR "+Columns.GLOBALSTEP.name()+" = "+TASKSTEP.TRANSFERTASK.ordinal();
              condition += " OR "+Columns.GLOBALSTEP.name()+" = "+TASKSTEP.POSTTASK.ordinal()+" )";
          }
          if (error) {
              if (condition != null) {
                  condition += " AND ";
              } else {
                  condition = "";
              }
              condition += Columns.GLOBALSTEP.name()+" = "+TASKSTEP.ERRORTASK.ordinal();
          }
          if (done) {
              if (condition != null) {
                  condition += " AND ";
              } else {
                  condition = "";
              }
              condition += Columns.GLOBALSTEP.name()+" = "+TASKSTEP.ALLDONETASK.ordinal();
          }
      }
      preparedStatement.createPrepareStatement(request+condition+
              " ORDER BY "+Columns.START.name());
      int rank = 1;
      try {
          if (start != null & stop != null) {
              preparedStatement.getPreparedStatement().setTimestamp(rank, start);
              rank++;
              preparedStatement.getPreparedStatement().setTimestamp(rank, stop);
              rank++;
          } else if (start != null) {
              preparedStatement.getPreparedStatement().setTimestamp(rank, start);
              rank++;
          } else if (stop != null) {
              preparedStatement.getPreparedStatement().setTimestamp(rank, stop);
              rank++;
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
    * @return the DbPreparedStatement for getting Updated Object
    * @throws OpenR66DatabaseNoConnectionError
    * @throws OpenR66DatabaseSqlError
    */
   public static DbPreparedStatement getUpdatedPrepareStament(DbSession session) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
       String request = "SELECT " +selectAllFields;
       request += " FROM "+table+
           " WHERE "+Columns.UPDATEDINFO.name()+" = "+
           AbstractDbData.UpdatedInfo.UPDATED.ordinal();
       return new DbPreparedStatement(session, request);
   }
   /**
   *
   * @param session
   * @param start
   * @param stop
   * @return the DbPreparedStatement for getting Selected Object, already executed
   * @throws OpenR66DatabaseNoConnectionError
   * @throws OpenR66DatabaseSqlError
   */
  public static DbPreparedStatement getLogPrepareStament(DbSession session,
          Timestamp start, Timestamp stop)
      throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
      DbPreparedStatement preparedStatement = new DbPreparedStatement(session);
      String request = "SELECT " +selectAllFields+" FROM "+table;
      if (start != null & stop != null) {
          request += " WHERE "+Columns.START.name()+" >= ? AND "+
              Columns.START.name()+" <= ? ORDER BY "+Columns.START.name();
          preparedStatement.createPrepareStatement(request);
          try {
            preparedStatement.getPreparedStatement().setTimestamp(1, start);
            preparedStatement.getPreparedStatement().setTimestamp(2, stop);
        } catch (SQLException e) {
            preparedStatement.realClose();
            throw new OpenR66DatabaseSqlError(e);
        }
      } else if (start != null) {
          request += " WHERE "+Columns.START.name()+" >= ? ORDER BY "+Columns.START.name();
          preparedStatement.createPrepareStatement(request);
          try {
              preparedStatement.getPreparedStatement().setTimestamp(1, start);
          } catch (SQLException e) {
              preparedStatement.realClose();
              throw new OpenR66DatabaseSqlError(e);
          }
      } else if (stop != null) {
          request += " WHERE "+Columns.START.name()+" <= ? ORDER BY "+Columns.START.name();
          preparedStatement.createPrepareStatement(request);
          try {
              preparedStatement.getPreparedStatement().setTimestamp(1, stop);
          } catch (SQLException e) {
              preparedStatement.realClose();
              throw new OpenR66DatabaseSqlError(e);
          }
      } else {
          request += " ORDER BY "+Columns.START.name();
          preparedStatement.createPrepareStatement(request);
      }
      return preparedStatement;
  }
  /**
  * purge in same interval all runners with globallaststep
  * as ALLDONETASK or ERRORTASK
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
     String request = "DELETE FROM "+table+ " WHERE ("+
         Columns.GLOBALLASTSTEP+" = "+TASKSTEP.ALLDONETASK.ordinal()+" OR "+
         Columns.GLOBALLASTSTEP+" = "+TASKSTEP.ERRORTASK.ordinal()+") ";
     if (start != null & stop != null) {
         request += " AND "+ Columns.START.name()+" >= ? AND "+
             Columns.STOP.name()+" <= ? ";
         preparedStatement.createPrepareStatement(request);
         try {
           preparedStatement.getPreparedStatement().setTimestamp(1, start);
           preparedStatement.getPreparedStatement().setTimestamp(2, stop);
       } catch (SQLException e) {
           preparedStatement.realClose();
           throw new OpenR66DatabaseSqlError(e);
       }
     } else if (start != null) {
         request += " AND "+ Columns.START.name()+" >= ? ";
         preparedStatement.createPrepareStatement(request);
         try {
             preparedStatement.getPreparedStatement().setTimestamp(1, start);
         } catch (SQLException e) {
             preparedStatement.realClose();
             throw new OpenR66DatabaseSqlError(e);
         }
     } else if (stop != null) {
         request += " AND "+ Columns.STOP.name()+" <= ? ";
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
     logger.info("Purge "+nb+" from "+request);
     preparedStatement.realClose();
     return nb;
 }
   /**
    * Change TORUN to UPDATED TaskRunner from database
    * @param session
    * @throws OpenR66DatabaseNoConnectionError
    */
   public static void changeToRunToUpdated(DbSession session) throws OpenR66DatabaseNoConnectionError {
       // Change TORUN to UPDATED since they should ready
       String request = "UPDATE "+table+" SET " +
           Columns.UPDATEDINFO.name()+ "="+
           AbstractDbData.UpdatedInfo.UPDATED.ordinal();
       request += " WHERE "+Columns.UPDATEDINFO.name()+" = "+
           AbstractDbData.UpdatedInfo.TORUN.ordinal();
       DbPreparedStatement initial = new DbPreparedStatement(session);
       try {
           initial.createPrepareStatement(request);
           initial.executeUpdate();
       } catch (OpenR66DatabaseNoConnectionError e) {
           logger.error("Cannot execute Commander", e);
           return;
       } catch (OpenR66DatabaseSqlError e) {
           logger.error("Cannot execute Commander", e);
           return;
       } finally {
           initial.close();
       }
   }
   /**
    * Change TORUN to UPDATED TaskRunner from database
    * @param session
    * @throws OpenR66DatabaseNoConnectionError
    */
   public static void changeFinishedToDone(DbSession session) throws OpenR66DatabaseNoConnectionError {
       // Update all UpdatedInfo to DONE where GlobalLastStep = ALLDONETASK and status = CompleteOk
       String request = "UPDATE "+table+" SET " +
           Columns.UPDATEDINFO.name()+ "="+
           AbstractDbData.UpdatedInfo.DONE.ordinal();
       request += " WHERE "+Columns.UPDATEDINFO.name()+" <> "+
           AbstractDbData.UpdatedInfo.DONE.ordinal() +" AND "+
           Columns.UPDATEDINFO.name()+" > 0 AND "+
           Columns.GLOBALLASTSTEP.name()+" = "+TASKSTEP.ALLDONETASK.ordinal()+
           " AND "+Columns.STEPSTATUS.name()+" = '"+ErrorCode.CompleteOk.getCode()+"'";
       DbPreparedStatement initial = new DbPreparedStatement(session);
       try {
           initial.createPrepareStatement(request);
           initial.executeUpdate();
       } catch (OpenR66DatabaseNoConnectionError e) {
           logger.error("Cannot execute Commander", e);
           return;
       } catch (OpenR66DatabaseSqlError e) {
           logger.error("Cannot execute Commander", e);
           return;
       } finally {
           initial.close();
       }
   }
   public boolean restart() {
       // Restart if already stopped and not finished
       if (this.getStatus() != ErrorCode.CompleteOk) {
           // restart
           switch (TASKSTEP.values()[this.getGloballaststep()]) {
               case PRETASK:
                   // restart
                   this.setPreTask(0);
                   this.setExecutionStatus(ErrorCode.InitOk);
                   break;
               case TRANSFERTASK:
                   // continue
                   this.setTransferTask(this.getRank());
                   this.setExecutionStatus(ErrorCode.PreProcessingOk);
                   break;
               case POSTTASK:
                   // restart
                   this.setPostTask(0);
                   this.setExecutionStatus(ErrorCode.TransferOk);
                   break;
           }
           this.changeUpdatedInfo(UpdatedInfo.UPDATED);
           try {
               this.saveStatus();
           } catch (OpenR66RunnerErrorException e) {
               return false;
           }
           return true;
       } else {
           // Already finished so DONE
           this.changeUpdatedInfo(UpdatedInfo.DONE);
           try {
               this.saveStatus();
           } catch (OpenR66RunnerErrorException e) {
           }
           return false;
       }
   }
    /*
     * (non-Javadoc)
     *
     * @see openr66.database.data.AbstractDbData#changeUpdatedInfo(UpdatedInfo)
     */
    @Override
    public void changeUpdatedInfo(UpdatedInfo info) {
        if (updatedInfo != info.ordinal()) {
            updatedInfo = info.ordinal();
            allFields[Columns.UPDATEDINFO.ordinal()].setValue(updatedInfo);
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
        return globalstep == TASKSTEP.TRANSFERTASK.ordinal();
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
     * @return True if this runner is finished, either in success or in error
     */
    public boolean isFinished() {
        return globalstep == TASKSTEP.ALLDONETASK.ordinal() ||
        (globalstep == TASKSTEP.ERRORTASK.ordinal() && status != ErrorCode.Running);
    }
    /**
     * Set Pre Task step
     * @param step
     */
    public void setPreTask(int step) {
        globalstep = TASKSTEP.PRETASK.ordinal();
        globallaststep = TASKSTEP.PRETASK.ordinal();
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(globalstep);
        allFields[Columns.GLOBALLASTSTEP.ordinal()].setValue(globallaststep);
        this.step = step;
        allFields[Columns.STEP.ordinal()].setValue(this.step);
        status = ErrorCode.Running;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(status.getCode());
        isSaved = false;
    }
    /**
     * Set Transfer rank
     * @param rank
     */
    public void setTransferTask(int rank) {
        globalstep = TASKSTEP.TRANSFERTASK.ordinal();
        globallaststep = TASKSTEP.TRANSFERTASK.ordinal();
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(globalstep);
        allFields[Columns.GLOBALLASTSTEP.ordinal()].setValue(globallaststep);
        this.rank = rank;
        allFields[Columns.RANK.ordinal()].setValue(this.rank);
        status = ErrorCode.Running;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(status.getCode());
        isSaved = false;
    }
    /**
     * Set the status of the transfer
     * @param status True if success
     * @return the current rank of transfer
     */
    public int finishTransferTask(boolean status) {
        if (status) {
            this.status = ErrorCode.TransferOk;
        } else {
            if (this.status == ErrorCode.InitOk ||
                    this.status == ErrorCode.PostProcessingOk ||
                    this.status == ErrorCode.PreProcessingOk ||
                    this.status == ErrorCode.Running ||
                    this.status == ErrorCode.TransferOk) {
                this.status = ErrorCode.TransferError;
            }
        }
        allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.getCode());
        isSaved = false;
        return rank;
    }
    /**
     * Set the Post Task step
     * @param step
     */
    public void setPostTask(int step) {
        globalstep = TASKSTEP.POSTTASK.ordinal();
        globallaststep = TASKSTEP.POSTTASK.ordinal();
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(globalstep);
        allFields[Columns.GLOBALLASTSTEP.ordinal()].setValue(globallaststep);
        this.step = step;
        allFields[Columns.STEP.ordinal()].setValue(this.step);
        status = ErrorCode.Running;
        allFields[Columns.STEPSTATUS.ordinal()].setValue(status.getCode());
        isSaved = false;
    }
    /**
     * Set the Error Task step
     * @param step
     */
    public void setErrorTask(int step) {
        globalstep = TASKSTEP.ERRORTASK.ordinal();
        allFields[Columns.GLOBALSTEP.ordinal()].setValue(globalstep);
        this.step = step;
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
        if (status == ErrorCode.PostProcessingOk) {
            status = ErrorCode.CompleteOk;
        }
        allFields[Columns.STEPSTATUS.ordinal()].setValue(status.getCode());
        changeUpdatedInfo(UpdatedInfo.DONE);
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
     * @throws OpenR66RunnerEndTasksException
     */
    private R66Future runNext() throws
            OpenR66RunnerErrorException, OpenR66RunnerEndTasksException {
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
                        status = ErrorCode.PreProcessingOk;
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
                        status = ErrorCode.PostProcessingOk;
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
                allFields[Columns.STEP.ordinal()].setValue(step);
                allFields[Columns.STEPSTATUS.ordinal()].setValue(status.getCode());
                isSaved = false;
                return;
            } catch (OpenR66RunnerErrorException e) {
                status = ErrorCode.ExternalOp;
                allFields[Columns.STEP.ordinal()].setValue(step);
                allFields[Columns.STEPSTATUS.ordinal()].setValue(status.getCode());
                isSaved = false;
                throw new OpenR66RunnerErrorException("Runner is in error: " +
                        e.getMessage(), e);
            }
            if (future.isCancelled()) {
                status = future.getResult().code;
                allFields[Columns.STEP.ordinal()].setValue(step);
                allFields[Columns.STEPSTATUS.ordinal()].setValue(status.getCode());
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
            if (! isSender) {
                // flush partial file
                session.getFile().flush();
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
                .debug("{} {} ", GgInternalLogger.getRankMethodAndLine(3),
                        this);
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
        if ((! isSender()) && getRank() == 0) {
            try {
                R66File file = session.getFile();
                if (file != null) {
                    file.delete();
                }
            } catch (CommandAbstractException e1) {
                logger.warn("Cannot delete temporary empty file", e1);
            }
        }
    }
    @Override
    public String toString() {
        return "Run: " + (rule != null? rule.toString() : ruleId) + " on " +
                filename + " STEP: " + TASKSTEP.values()[globalstep] +
                "("+TASKSTEP.values()[globallaststep]+ "):" + step + ":" +
                status.mesg +
                " Transfer Rank: " + rank + " SpecialId: " + specialId + " isSender: " +
                isSender + " isMoved: " + isFileMoved+" Mode: "+TRANSFERMODE.values()[mode]+
                " Requester: "+requesterHostId+" Requested: "+requestedHostId+
                " Start: "+start+" Stop: "+stop+" Info: "+UpdatedInfo.values()[updatedInfo];
    }
    /**
     *
     * @return the header for a table of runners in Html format
     */
    public static String headerHtml() {
        return "<td>SpecialId</td><td>Rule</td><td>Filename" +
            "</td><td>Step (LastStep)</td><td>Action</td><td>Status" +
            "</td><td>Transfer Rank</td><td>isMoved" +
            "</td><td>Requester</td><td>Requested"+
            "</td><td>Start</td><td>Stop</td><td>Bandwidth (Kbits)</td><td>Free Space(MB)</td>";
    }
    /**
     * @param session
     * @return The associated freespace of the current directory
     */
    public long freespace(R66Session session) {
        long freespace = -1;
        DbRule rule = null;
        try {
            rule = (this.rule != null) ? this.rule : new DbRule(this.dbSession, this.ruleId);
        } catch (OpenR66DatabaseException e) {
        }
        if (this.rule == null) {
            this.rule = rule;
        }
        if (rule != null){
            if (! this.isSender) {
                try {
                    String sdir;
                    if (this.globallaststep == TASKSTEP.ALLDONETASK.ordinal()) {
                        // all finished
                        sdir = rule.recvPath;
                    } else if (this.globallaststep == TASKSTEP.POSTTASK.ordinal()) {
                        // Post task
                        sdir = rule.recvPath;
                    } else {
                        // are we in sending or receive
                        sdir = rule.workPath;
                    }
                    R66Dir dir;
                    if (HttpHandler.usedDir.containsKey(sdir)) {
                        dir = HttpHandler.usedDir.get(sdir);
                    } else {
                        dir = new R66Dir(session);
                        dir.changeDirectory(sdir);
                        HttpHandler.usedDir.put(sdir, dir);
                    }
                    freespace = dir.getFreeSpace() / 0x100000L;
                } catch (CommandAbstractException e) {
                    logger.warn("Error while freespace compute", e);
                }
            }
        }
        return freespace;
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
    /**
     * @param session
     * @return the runner in Html format compatible with the header from headerHtml method
     */
    public String toHtml(R66Session session) {
       long freespace = freespace(session);
       String color = getHtmlColor();
       return "<td>" + specialId +
                "</td><td>" + (rule != null? rule.toShortString() : ruleId) + "</td><td>" +
                filename + "</td><td bgcolor=\""+color+"\">" + TASKSTEP.values()[globalstep] +
                " ("+TASKSTEP.values()[globallaststep]+ ")</td><td>" + step + "</td><td>" +
                status.mesg +
                "</td><td>" + rank + "</td><td>" +
                isFileMoved+"</td><td>"+requesterHostId+"</td><td>"+requestedHostId+
                "</td><td>"+start+"</td><td>"+stop+"</td><td>"+
                (int)(((double) (rank*blocksize*8))/((double) (stop.getTime()+1-start.getTime())))
                +"</td>"+
                "<td>"+freespace+"</td>";
    }
    /**
     * @param session
     * @param body
     * @return the runner in Html format specified by body by replacing all instance of fields
     */
    public String toSpecializedHtml(R66Session session, String body) {
        long freespace = freespace(session);
        String line = body.replace("XXXSpecIdXXX", Long.toString(specialId));
        line = line.replace("XXXRulXXX", (rule != null? rule.toShortString() : ruleId));
        line = line.replace("XXXFileXXX", filename);
        line = line.replace("XXXStepXXX", TASKSTEP.values()[globalstep] +
                " ("+TASKSTEP.values()[globallaststep]+ ")");
        line = line.replace("XXXCOLXXX", getHtmlColor());
        line = line.replace("XXXActXXX", Integer.toString(step));
        line = line.replace("XXXStatXXX", status.mesg);
        line = line.replace("XXXBloXXX", Integer.toString(rank));
        line = line.replace("XXXisSendXXX", Boolean.toString(isSender));
        line = line.replace("XXXisMovXXX", Boolean.toString(isFileMoved));
        line = line.replace("XXXModXXX", TRANSFERMODE.values()[mode].toString());
        line = line.replace("XXXReqrXXX", requesterHostId);
        line = line.replace("XXXReqdXXX", requestedHostId);
        line = line.replace("XXXStarXXX", start.toString());
        line = line.replace("XXXStopXXX", stop.toString());
        line = line.replace("XXXBandXXX", Integer.toString(
                (int)(((double) (rank*blocksize*8))/((double) (stop.getTime()+1-start.getTime())))));
        line = line.replace("XXXFreeXXX", Long.toString(freespace));
        return line;
    }
    /**
     *
     * @return True if the current host is the requested host (to prevent request to itself)
     */
    public boolean isSelfRequested() {
        return (this.requestedHostId.equals(Configuration.configuration.HOST_ID)
                || this.requestedHostId.equals(Configuration.configuration.HOST_SSLID));
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
     * @return a Key representing the primary key as a unique string
     */
    public String getKey() {
        return requestedHostId+" "+requesterHostId+" "+specialId;
    }
    /**
     * Construct a new Element with value
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
     *
     * @param runner
     * @return The Element representing the given Runner
     * @throws OpenR66DatabaseSqlError
     */
    private static Element getElementFromRunner(DbTaskRunner runner) throws OpenR66DatabaseSqlError {
        Element root = new DefaultElement("runner");
        for (DbValue value: runner.allFields) {
            if (value.column.equals(Columns.UPDATEDINFO.name())) {
                continue;
            }
            root.add(newElement(value.column.toLowerCase(), value.getValueAsString()));
        }
        return root;
    }
    /**
     * Write the selected TaskRunners from PrepareStatement to a XML tree
     * @param preparedStatement ready to be executed
     * @return the generated Document
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public static Document writeXML(DbPreparedStatement preparedStatement) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        Document document = DocumentHelper.createDocument();
        Element root = document.addElement("taskrunners");
        preparedStatement.executeQuery();
        Element node;
        while (preparedStatement.getNext()) {
            DbTaskRunner runner = DbTaskRunner.getFromStatement(preparedStatement);
            node = DbTaskRunner.getElementFromRunner(runner);
            root.add(node);
        }
        return document;
    }
    /**
     * Write all TaskRunners to an XML file
     * @param filename
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public static void writeXML(String filename) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        String request = "SELECT " +DbTaskRunner.selectAllFields+" FROM "+DbTaskRunner.table;
        DbPreparedStatement preparedStatement = null;
        Document document = null;
        try {
            preparedStatement = new DbPreparedStatement(DbConstant.admin.session);
            preparedStatement.createPrepareStatement(request);
            document = writeXML(preparedStatement);
        } finally {
            if (preparedStatement != null) {
                preparedStatement.realClose();
            }
        }
        if (document != null) {
            try {
                FileUtils.writeXML(filename, null, document);
            } catch (OpenR66ProtocolSystemException e) {
                logger.warn("Cannot write XML file",e);
            }
        }
    }
}
