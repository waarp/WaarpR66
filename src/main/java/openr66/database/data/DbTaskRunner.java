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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;

import openr66.context.task.TaskRunner.TaskStatus;
import openr66.database.DbConstant;
import openr66.database.DbPreparedStatement;
import openr66.database.DbSession;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseNoDataException;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.database.model.DbModelFactory;
import openr66.protocol.localhandler.packet.RequestPacket;

/**
 * @author Frederic Bregier
 *
 */
public class DbTaskRunner extends AbstractDbData {
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

    private int globalstep;

    private int step;

    private int rank;

    private TaskStatus status = TaskStatus.UNKNOWN;

    // FIXME need a special ID
    private long specialId;

    private boolean isRetrieve;

    private String filename;

    private boolean isFileMoved = false;

    private String ruleId;

    private int blocksize;

    private String originalFilename;

    private String fileInformation;

    private int mode;

    private Timestamp start;

    private Timestamp stop;

    private String requesterHostId;

    private String requestedHostId;

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

    /**
     *
     * @param specialId
     * @param globalstep
     * @param step
     * @param rank
     * @param status
     * @param isRetrieve
     * @param filename
     * @param isFileMoved
     * @param idRule
     * @param blocksize
     * @param originalName
     * @param fileInfo
     * @param mode
     * @param requesterHostId
     * @param requestedHostId
     */
    public DbTaskRunner(long specialId, int globalstep, int step, int rank, TaskStatus status,
            boolean isRetrieve, String filename,
            boolean isFileMoved, String idRule,
            int blocksize, String originalName, String fileInfo, int mode,
            String requesterHostId, String requestedHostId) {
        this.specialId = specialId;
        this.globalstep = globalstep;
        this.step = step;
        this.rank = rank;
        this.status = status;
        this.isRetrieve = isRetrieve;
        this.filename = filename;
        this.isFileMoved = isFileMoved;
        this.ruleId = idRule;
        this.start = new Timestamp(System.currentTimeMillis());
        this.blocksize = blocksize;
        this.originalFilename = originalName;
        this.fileInformation = fileInfo;
        this.mode = mode;
        this.requesterHostId = requesterHostId;
        this.requestedHostId = requestedHostId;
        this.updatedInfo = UNKNOWN;
        this.setToArray();
        this.isSaved = false;
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
     * @param specialId
     * @throws OpenR66DatabaseException
     */
    public DbTaskRunner(long specialId) throws OpenR66DatabaseException {
        this.specialId = specialId;
        // load from DB
        select();
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
     * @param GLOBALSTEP the GLOBALSTEP to set
     */
    public void setGlobalstep(int globalstep) {
        if (this.globalstep != globalstep) {
            this.globalstep = globalstep;
            allFields[Columns.GLOBALSTEP.ordinal()].setValue(this.globalstep);
            this.isSaved = false;
        }
    }

    /**
     * @param STEP the STEP to set
     */
    public void setStep(int step) {
        if (this.step != step) {
            this.step = step;
            allFields[Columns.STEP.ordinal()].setValue(this.step);
            this.isSaved = false;
        }
    }

    /**
     * @param RANK the RANK to set
     */
    public void setRank(int rank) {
        if (this.rank != rank) {
            this.rank = rank;
            allFields[Columns.RANK.ordinal()].setValue(this.rank);
            this.isSaved = false;
        }
    }

    /**
     * @param status the status to set
     */
    public void setStatus(TaskStatus status) {
        if (this.status != status) {
            this.status = status;
            allFields[Columns.STEPSTATUS.ordinal()].setValue(this.status.ordinal());
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
     * @return the globalstep
     */
    public int getGlobalstep() {
        return globalstep;
    }

    /**
     * @return the step
     */
    public int getStep() {
        return step;
    }

    /**
     * @return the rank
     */
    public int getRank() {
        return rank;
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
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * @return the isFileMoved
     */
    public boolean isFileMoved() {
        return isFileMoved;
    }

    /**
     * @return the ruleId
     */
    public String getRuleId() {
        return ruleId;
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
     * @return the mode
     */
    public int getMode() {
        return mode;
    }

    /**
     * @return the specialId
     */
    public long getSpecialId() {
        return specialId;
    }

    /**
     * @return the requesterHostId
     */
    public String getRequesterHostId() {
        return requesterHostId;
    }

    /**
     * @return the requestedHostId
     */
    public String getRequestedHostId() {
        return requestedHostId;
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
        try {
            runner = new DbTaskRunner(DbConstant.ILLEGALVALUE);
        } catch (OpenR66DatabaseException e) {
            // ignore
        }
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
