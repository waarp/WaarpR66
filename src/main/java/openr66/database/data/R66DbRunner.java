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

import openr66.database.DbConstant;
import openr66.database.R66DbPreparedStatement;
import openr66.database.R66DbSession;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseNoDataException;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.filesystem.R66Rule;
import openr66.filesystem.R66Session;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.task.TaskRunner;
import openr66.task.TaskRunner.TaskStatus;

/**
 * @author Frederic Bregier
 *
 */
public class R66DbRunner extends AbstractDbData {
    public static enum Columns {
        globalstep, step, rank, stepstatus, retrievemode, filename, ismoved, idrule,
        blocksize, originalname, fileinfo, mode,
        start, stop,
        updatedinfo,
        specialid;
    }
    public static int [] dbTypes = {
        Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.BIT,
        Types.VARCHAR, Types.BIT, Types.VARCHAR,
        Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
        Types.TIMESTAMP, Types.TIMESTAMP,
        Types.INTEGER, Types.VARCHAR
    };
    public static String table = " runner ";
    public static String tableseq = " cptrunner ";
    public DbValue sequence = new DbValue(Long.MIN_VALUE,"runSeq");


    private int globalstep;

    private int step;

    private int rank;

    private TaskStatus status;

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

    private int updatedInfo;

    private boolean isSaved = false;

    // ALL TABLE SHOULD IMPLEMENT THIS
    private DbValue primaryKey = new DbValue(specialId, Columns.specialid.name());
    private DbValue[] otherFields = {
      // globalstep, step, rank, stepstatus, retrievemode, filename, ismoved, idrule,
      // blocksize, originalname, fileinfo, mode,
      // start, stop
      // updatedinfo
      new DbValue(globalstep, Columns.globalstep.name()),
      new DbValue(step, Columns.step.name()),
      new DbValue(rank, Columns.rank.name()),
      new DbValue(status.ordinal(), Columns.stepstatus.name()),
      new DbValue(isRetrieve, Columns.retrievemode.name()),
      new DbValue(filename, Columns.filename.name()),
      new DbValue(isFileMoved, Columns.ismoved.name()),
      new DbValue(ruleId, Columns.idrule.name()),
      new DbValue(blocksize, Columns.blocksize.name()),
      new DbValue(originalFilename, Columns.originalname.name()),
      new DbValue(fileInformation, Columns.fileinfo.name()),
      new DbValue(mode, Columns.mode.name()),
      new DbValue(start, Columns.start.name()),
      new DbValue(stop, Columns.stop.name()),
      new DbValue(updatedInfo, Columns.updatedinfo.name())
    };
    private DbValue[] allFields = {
      otherFields[0], otherFields[1], otherFields[2], otherFields[3],
      otherFields[4], otherFields[5], otherFields[6], otherFields[7], otherFields[8],
      otherFields[9], otherFields[10], otherFields[11], otherFields[12], otherFields[13],
      otherFields[14],
      primaryKey
    };
    private static final String selectAllFields =
        Columns.globalstep.name()+","+Columns.step.name()+
        ","+Columns.rank.name()+","+Columns.stepstatus.name()+","+Columns.retrievemode.name()+
        ","+Columns.filename.name()+","+Columns.ismoved.name()+","+Columns.idrule.name()+
        ","+Columns.blocksize.name()+","+Columns.originalname.name()+","+Columns.fileinfo.name()+
        ","+Columns.mode.name()+
        ","+Columns.start.name()+","+Columns.stop.name()+
        ","+Columns.updatedinfo.name()+
        ","+Columns.specialid.name();
    private static final String updateAllFields =
        Columns.globalstep.name()+"=?,"+Columns.step.name()+
        "=?,"+Columns.rank.name()+"=?,"+Columns.stepstatus.name()+"=?,"+Columns.retrievemode.name()+
        "=?,"+Columns.filename.name()+"=?,"+Columns.ismoved.name()+"=?,"+Columns.idrule.name()+
        "=?,"+Columns.blocksize.name()+"=?,"+Columns.originalname.name()+"=?,"+Columns.fileinfo.name()+
        "=?,"+Columns.mode.name()+
        "=?,"+Columns.start.name()+"=?,"+Columns.stop.name()+"=?,"+Columns.updatedinfo.name()+"=?";
    private static final String insertAllValues = " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";

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
     * @param updatedInfo
     */
    public R66DbRunner(long specialId, int globalstep, int step, int rank, TaskStatus status,
            boolean isRetrieve, String filename,
            boolean isFileMoved, String idRule,
            int blocksize, String originalName, String fileInfo, int mode, int updatedInfo) {
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
        this.updatedInfo = updatedInfo;
        this.setToArray();
        this.isSaved = false;
    }

    @Override
    protected void setToArray() {
        allFields[Columns.specialid.ordinal()].setValue(this.specialId);
        allFields[Columns.globalstep.ordinal()].setValue(this.globalstep);
        allFields[Columns.step.ordinal()].setValue(this.step);
        allFields[Columns.rank.ordinal()].setValue(this.rank);
        allFields[Columns.stepstatus.ordinal()].setValue(this.status.ordinal());
        allFields[Columns.retrievemode.ordinal()].setValue(this.isRetrieve);
        allFields[Columns.filename.ordinal()].setValue(this.filename);
        allFields[Columns.ismoved.ordinal()].setValue(this.isFileMoved);
        allFields[Columns.idrule.ordinal()].setValue(this.ruleId);
        allFields[Columns.blocksize.ordinal()].setValue(this.blocksize);
        allFields[Columns.originalname.ordinal()].setValue(this.originalFilename);
        allFields[Columns.fileinfo.ordinal()].setValue(this.fileInformation);
        allFields[Columns.mode.ordinal()].setValue(this.mode);
        allFields[Columns.start.ordinal()].setValue(this.start);
        this.stop = new Timestamp(System.currentTimeMillis());
        allFields[Columns.stop.ordinal()].setValue(this.stop);
        allFields[Columns.updatedinfo.ordinal()].setValue(this.updatedInfo);
    }
    @Override
    protected void setFromArray() throws OpenR66DatabaseSqlError {
        this.specialId = (Long) allFields[Columns.specialid.ordinal()].getValue();
        this.globalstep = (Integer) allFields[Columns.globalstep.ordinal()].getValue();
        this.step = (Integer) allFields[Columns.step.ordinal()].getValue();
        this.rank = (Integer) allFields[Columns.rank.ordinal()].getValue();
        this.status = TaskStatus.values()[((Integer) allFields[Columns.stepstatus.ordinal()].getValue())];
        this.isRetrieve = (Boolean) allFields[Columns.retrievemode.ordinal()].getValue();
        this.filename = (String) allFields[Columns.filename.ordinal()].getValue();
        this.isFileMoved = (Boolean) allFields[Columns.ismoved.ordinal()].getValue();
        this.ruleId = (String) allFields[Columns.idrule.ordinal()].getValue();
        this.blocksize = (Integer) allFields[Columns.blocksize.ordinal()].getValue();
        this.originalFilename = (String) allFields[Columns.originalname.ordinal()].getValue();
        this.fileInformation = (String) allFields[Columns.fileinfo.ordinal()].getValue();
        this.mode = (Integer) allFields[Columns.mode.ordinal()].getValue();
        this.start = (Timestamp) allFields[Columns.start.ordinal()].getValue();
        this.stop = (Timestamp) allFields[Columns.stop.ordinal()].getValue();
        this.updatedInfo = (Integer) allFields[Columns.updatedinfo.ordinal()].getValue();
    }
    /**
     * @param specialId
     * @throws OpenR66DatabaseException
     */
    public R66DbRunner(long specialId) throws OpenR66DatabaseException {
        this.specialId = specialId;
        // load from DB
        select();
    }

    /* (non-Javadoc)
     * @see openr66.database.data.AbstractDbData#delete()
     */
    @Override
    public void delete() throws OpenR66DatabaseException {
        R66DbPreparedStatement preparedStatement =
            new R66DbPreparedStatement(DbConstant.admin.session);
        try {
            preparedStatement.createPrepareStatement("DELETE FROM "+
                    table+" WHERE "+Columns.specialid.name()+" = ?");
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
            // Special table for that
            R66DbPreparedStatement preparedStatement =
                new R66DbPreparedStatement(DbConstant.admin.session);
            try {
                preparedStatement.createPrepareStatement("SELECT "+sequence.column+
                        " FROM "+tableseq+" FOR UPDATE");
                preparedStatement.executeQuery();
                if (preparedStatement.getNext()) {
                    this.getValue(preparedStatement, sequence);
                    this.specialId = ((Long)sequence.value)+1;
                    sequence.setValue(this.specialId);
                    preparedStatement.realClose();
                    preparedStatement =
                        new R66DbPreparedStatement(DbConstant.admin.session);
                    preparedStatement.createPrepareStatement("UPDATE "+tableseq+
                            " SET "+sequence.column+"=?");
                    this.setValue(preparedStatement, sequence);
                    int count = preparedStatement.executeUpdate();
                    preparedStatement.realClose();
                    if (count <= 0) {
                        throw new OpenR66DatabaseNoDataException("No row found");
                    }
                    primaryKey.setValue(this.specialId);
                } else {
                    throw new OpenR66DatabaseNoDataException("No row found");
                }
            } finally {
                preparedStatement.realClose();
            }
        }
        R66DbPreparedStatement preparedStatement =
            new R66DbPreparedStatement(DbConstant.admin.session);
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
        R66DbPreparedStatement preparedStatement =
            new R66DbPreparedStatement(DbConstant.admin.session);
        preparedStatement.createPrepareStatement("SELECT "+selectAllFields+" FROM "+
                table+" WHERE "+Columns.specialid.name()+" = ?");
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
        R66DbPreparedStatement preparedStatement =
            new R66DbPreparedStatement(DbConstant.admin.session);
        try {
            preparedStatement.createPrepareStatement("UPDATE "+table+" SET "+updateAllFields+
                    " WHERE "+Columns.specialid.name()+" = ?");
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
            allFields[Columns.updatedinfo.ordinal()].setValue(this.updatedInfo);
            this.isSaved = false;
        }
    }

    /**
     * @param globalstep the globalstep to set
     */
    public void setGlobalstep(int globalstep) {
        if (this.globalstep != globalstep) {
            this.globalstep = globalstep;
            allFields[Columns.globalstep.ordinal()].setValue(this.globalstep);
            this.isSaved = false;
        }
    }

    /**
     * @param step the step to set
     */
    public void setStep(int step) {
        if (this.step != step) {
            this.step = step;
            allFields[Columns.step.ordinal()].setValue(this.step);
            this.isSaved = false;
        }
    }

    /**
     * @param rank the rank to set
     */
    public void setRank(int rank) {
        if (this.rank != rank) {
            this.rank = rank;
            allFields[Columns.rank.ordinal()].setValue(this.rank);
            this.isSaved = false;
        }
    }

    /**
     * @param status the status to set
     */
    public void setStatus(TaskStatus status) {
        if (this.status != status) {
            this.status = status;
            allFields[Columns.stepstatus.ordinal()].setValue(this.status.ordinal());
            this.isSaved = false;
        }
    }

    /**
     * @param filename the filename to set
     */
    public void setFilename(String filename) {
        if (this.filename != filename) {
            this.filename = filename;
            allFields[Columns.filename.ordinal()].setValue(this.filename);
            this.isSaved = false;
        }
    }

    /**
     * @param isFileMoved the isFileMoved to set
     */
    public void setFileMoved(boolean isFileMoved) {
        if (this.isFileMoved != isFileMoved) {
            this.isFileMoved = isFileMoved;
            allFields[Columns.ismoved.ordinal()].setValue(this.isFileMoved);
            this.isSaved = false;
        }
    }

    public TaskRunner getTaskRunner(R66Session session, R66Rule rule) {
        // FIXME
        return new TaskRunner(session, rule, this.specialId);
    }
    public RequestPacket getRequest() {
        // FIXME
        return new RequestPacket(this.ruleId,this.mode,this.originalFilename,this.blocksize,this.rank,
                this.specialId,this.fileInformation);
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
        R66DbPreparedStatement preparedStatement =
            new R66DbPreparedStatement(DbConstant.admin.session);
        R66DbRunner runner = null;
        try {
            runner = new R66DbRunner(DbConstant.ILLEGALVALUE);
        } catch (OpenR66DatabaseException e) {
            // ignore
        }
        try {
            if (status == UNKNOWN) {
                preparedStatement.createPrepareStatement("SELECT COUNT("+runner.primaryKey+") FROM "+
                        table);
            } else {
                preparedStatement.createPrepareStatement("SELECT COUNT("+runner.primaryKey+") FROM "+
                        table+" WHERE "+Columns.updatedinfo.name()+" = ?");
                runner.allFields[Columns.updatedinfo.ordinal()].setValue(status);
                runner.setValue(preparedStatement, runner.allFields[Columns.updatedinfo.ordinal()]);
            }
            preparedStatement.executeQuery();
            int count = 0;
            if (preparedStatement.getNext()) {
                ResultSet rs = preparedStatement.getResultSet();
                try {
                    count = rs.getInt(1);
                } catch (SQLException e) {
                    R66DbSession.error(e);
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
                        table+" WHERE "+Columns.updatedinfo.name()+" = ?");
                runner.allFields[Columns.updatedinfo.ordinal()].setValue(status);
                runner.setValue(preparedStatement, runner.allFields[Columns.updatedinfo.ordinal()]);
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
