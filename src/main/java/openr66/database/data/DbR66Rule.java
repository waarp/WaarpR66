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

import java.sql.Types;

import openr66.database.DbConstant;
import openr66.database.DbPreparedStatement;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseNoDataException;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.filesystem.R66Rule;

/**
 * @author Frederic Bregier
 *
 */
public class DbR66Rule extends AbstractDbData {
    public static enum Columns {
        HOSTIDS, RECVPATH, SENDPATH, ARCHIVEPATH, WORKPATH, PRETASKS, POSTTASKS, ERRORTASKS,
        UPDATEDINFO,
        IDRULE
    }
    public static int [] dbTypes = {
        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
        Types.INTEGER, Types.VARCHAR
    };
    public static String table = " RULES ";


    /**
     * Global Id
     */
    public String idRule = null;

    /**
     * The Name addresses (serverIds)
     */
    public String ids = null;

    /**
     * The associated Recv Path
     */
    public String recvPath = null;

    /**
     * The associated Send Path
     */
    public String sendPath = null;

    /**
     * The associated Archive Path
     */
    public String archivePath = null;

    /**
     * The associated Work Path
     */
    public String workPath = null;

    /**
     * The associated Pre Tasks
     */
    public String preTasks = null;
    /**
     * The associated Post Tasks
     */
    public String postTasks = null;
    /**
     * The associated Error Tasks
     */
    public String errorTasks = null;

    private int updatedInfo;

    private boolean isSaved = false;

    // ALL TABLE SHOULD IMPLEMENT THIS
    private DbValue primaryKey = new DbValue(idRule, Columns.IDRULE.name());
    private DbValue[] otherFields = {
      // HOSTIDS, RECVPATH, SENDPATH, ARCHIVEPATH, WORKPATH, PRETASKS, POSTTASKS, ERRORTASKS
      new DbValue(ids, Columns.HOSTIDS.name()),
      new DbValue(recvPath, Columns.RECVPATH.name()),
      new DbValue(sendPath, Columns.SENDPATH.name()),
      new DbValue(archivePath, Columns.ARCHIVEPATH.name()),
      new DbValue(workPath, Columns.WORKPATH.name()),
      new DbValue(preTasks, Columns.PRETASKS.name()),
      new DbValue(postTasks, Columns.POSTTASKS.name()),
      new DbValue(errorTasks, Columns.ERRORTASKS.name()),
      new DbValue(updatedInfo, Columns.UPDATEDINFO.name())
    };
    private DbValue[] allFields = {
      otherFields[0], otherFields[1], otherFields[2], otherFields[3],
      otherFields[4], otherFields[5], otherFields[6], otherFields[7], otherFields[8],
      primaryKey
    };
    private static final String selectAllFields =
        Columns.HOSTIDS.name()+","+Columns.RECVPATH.name()+
        ","+Columns.SENDPATH.name()+","+Columns.ARCHIVEPATH.name()+","+Columns.WORKPATH.name()+
        ","+Columns.PRETASKS.name()+","+Columns.POSTTASKS.name()+","+Columns.ERRORTASKS.name()+
        ","+Columns.UPDATEDINFO.name()+
        ","+Columns.IDRULE.name();
    private static final String updateAllFields =
        Columns.HOSTIDS.name()+"=?,"+Columns.RECVPATH.name()+
        "=?,"+Columns.SENDPATH.name()+"=?,"+Columns.ARCHIVEPATH.name()+"=?,"+Columns.WORKPATH.name()+
        "=?,"+Columns.PRETASKS.name()+"=?,"+Columns.POSTTASKS.name()+"=?,"+Columns.ERRORTASKS.name()+
        "=?,"+Columns.UPDATEDINFO.name()+"=?";
    private static final String insertAllValues = " (?,?,?,?,?,?,?,?,?,?) ";

    @Override
    protected void setToArray() {
        allFields[Columns.IDRULE.ordinal()].setValue(this.idRule);
        allFields[Columns.HOSTIDS.ordinal()].setValue(this.ids);
        allFields[Columns.RECVPATH.ordinal()].setValue(this.recvPath);
        allFields[Columns.SENDPATH.ordinal()].setValue(this.sendPath);
        allFields[Columns.ARCHIVEPATH.ordinal()].setValue(this.archivePath);
        allFields[Columns.WORKPATH.ordinal()].setValue(this.workPath);
        allFields[Columns.PRETASKS.ordinal()].setValue(this.preTasks);
        allFields[Columns.POSTTASKS.ordinal()].setValue(this.postTasks);
        allFields[Columns.ERRORTASKS.ordinal()].setValue(this.errorTasks);
        allFields[Columns.UPDATEDINFO.ordinal()].setValue(this.updatedInfo);
    }
    @Override
    protected void setFromArray() throws OpenR66DatabaseSqlError {
        this.idRule = (String) allFields[Columns.IDRULE.ordinal()].getValue();
        this.ids = (String) allFields[Columns.HOSTIDS.ordinal()].getValue();
        this.recvPath = (String) allFields[Columns.RECVPATH.ordinal()].getValue();
        this.sendPath = (String) allFields[Columns.SENDPATH.ordinal()].getValue();
        this.archivePath = (String) allFields[Columns.ARCHIVEPATH.ordinal()].getValue();
        this.workPath = (String) allFields[Columns.WORKPATH.ordinal()].getValue();
        this.preTasks = (String) allFields[Columns.PRETASKS.ordinal()].getValue();
        this.postTasks = (String) allFields[Columns.POSTTASKS.ordinal()].getValue();
        this.errorTasks = (String) allFields[Columns.ERRORTASKS.ordinal()].getValue();
        this.updatedInfo = (Integer) allFields[Columns.UPDATEDINFO.ordinal()].getValue();
    }


    /**
     * @param idRule
     * @param ids
     * @param recvPath
     * @param sendPath
     * @param archivePath
     * @param workPath
     * @param preTasks
     * @param postTasks
     * @param errorTasks
     * @param updatedInfo
     */
    public DbR66Rule(String idRule, String ids, String recvPath,
            String sendPath, String archivePath, String workPath,
            String preTasks, String postTasks, String errorTasks,
            int updatedInfo) {
        this.idRule = idRule;
        this.ids = ids;
        this.recvPath = recvPath;
        this.sendPath = sendPath;
        this.archivePath = archivePath;
        this.workPath = workPath;
        this.preTasks = preTasks;
        this.postTasks = postTasks;
        this.errorTasks = errorTasks;
        this.updatedInfo = updatedInfo;
        this.setToArray();
        this.isSaved = false;
    }

    /**
     * @param idRule
     * @throws OpenR66DatabaseException
     */
    public DbR66Rule(String idRule) throws OpenR66DatabaseException {
        this.idRule = idRule;
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
                    table+" WHERE "+Columns.IDRULE.name()+" = ?");
            primaryKey.setValue(idRule);
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
        DbPreparedStatement preparedStatement =
            new DbPreparedStatement(DbConstant.admin.session);
        try {
            preparedStatement.createPrepareStatement("INSERT INTO "+table+" ("+selectAllFields+
                    ") VALUES "+insertAllValues);
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
        try {
            preparedStatement.createPrepareStatement("SELECT "+selectAllFields+" FROM "+
                    table+" WHERE "+Columns.IDRULE.name()+" = ?");
            primaryKey.setValue(idRule);
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
    public void update() throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError, OpenR66DatabaseNoDataException {
        if (this.isSaved) {
            return;
        }
        DbPreparedStatement preparedStatement =
            new DbPreparedStatement(DbConstant.admin.session);
        try {
            preparedStatement.createPrepareStatement("UPDATE "+table+" SET "+updateAllFields+
                    " WHERE "+Columns.IDRULE.name()+" = ?");
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

    public R66Rule getR66Rule() {
        return new R66Rule(this.idRule, this.ids, this.recvPath, this.sendPath,
                this.archivePath, this.workPath, this.preTasks, this.postTasks, this.errorTasks);
    }
}
