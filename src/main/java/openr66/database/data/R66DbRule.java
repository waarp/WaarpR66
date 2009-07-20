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
import openr66.database.R66DbPreparedStatement;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseNoDataException;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.filesystem.R66Rule;

/**
 * @author Frederic Bregier
 *
 */
public class R66DbRule extends AbstractDbData {
    public static enum Columns {
        hostids, recvpath, sendpath, archivepath, workpath, pretasks, posttasks, errortasks,
        updatedinfo,
        idrule
    }
    public static int [] dbTypes = {
        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
        Types.INTEGER, Types.VARCHAR
    };
    public static String table = " rules ";


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
    private DbValue primaryKey = new DbValue(idRule, Columns.idrule.name());
    private DbValue[] otherFields = {
      // hostids, recvpath, sendpath, archivepath, workpath, pretasks, posttasks, errortasks
      new DbValue(ids, Columns.hostids.name()),
      new DbValue(recvPath, Columns.recvpath.name()),
      new DbValue(sendPath, Columns.sendpath.name()),
      new DbValue(archivePath, Columns.archivepath.name()),
      new DbValue(workPath, Columns.workpath.name()),
      new DbValue(preTasks, Columns.pretasks.name()),
      new DbValue(postTasks, Columns.posttasks.name()),
      new DbValue(errorTasks, Columns.errortasks.name()),
      new DbValue(updatedInfo, Columns.updatedinfo.name())
    };
    private DbValue[] allFields = {
      otherFields[0], otherFields[1], otherFields[2], otherFields[3],
      otherFields[4], otherFields[5], otherFields[6], otherFields[7], otherFields[8],
      primaryKey
    };
    private static final String selectAllFields =
        Columns.hostids.name()+","+Columns.recvpath.name()+
        ","+Columns.sendpath.name()+","+Columns.archivepath.name()+","+Columns.workpath.name()+
        ","+Columns.pretasks.name()+","+Columns.posttasks.name()+","+Columns.errortasks.name()+
        ","+Columns.updatedinfo.name()+
        ","+Columns.idrule.name();
    private static final String updateAllFields =
        Columns.hostids.name()+"=?,"+Columns.recvpath.name()+
        "=?,"+Columns.sendpath.name()+"=?,"+Columns.archivepath.name()+"=?,"+Columns.workpath.name()+
        "=?,"+Columns.pretasks.name()+"=?,"+Columns.posttasks.name()+"=?,"+Columns.errortasks.name()+
        "=?,"+Columns.updatedinfo.name()+"=?";
    private static final String insertAllValues = " (?,?,?,?,?,?,?,?,?,?) ";

    @Override
    protected void setToArray() {
        allFields[Columns.idrule.ordinal()].setValue(this.idRule);
        allFields[Columns.hostids.ordinal()].setValue(this.ids);
        allFields[Columns.recvpath.ordinal()].setValue(this.recvPath);
        allFields[Columns.sendpath.ordinal()].setValue(this.sendPath);
        allFields[Columns.archivepath.ordinal()].setValue(this.archivePath);
        allFields[Columns.workpath.ordinal()].setValue(this.workPath);
        allFields[Columns.pretasks.ordinal()].setValue(this.preTasks);
        allFields[Columns.posttasks.ordinal()].setValue(this.postTasks);
        allFields[Columns.errortasks.ordinal()].setValue(this.errorTasks);
        allFields[Columns.updatedinfo.ordinal()].setValue(this.updatedInfo);
    }
    @Override
    protected void setFromArray() throws OpenR66DatabaseSqlError {
        this.idRule = (String) allFields[Columns.idrule.ordinal()].getValue();
        this.ids = (String) allFields[Columns.hostids.ordinal()].getValue();
        this.recvPath = (String) allFields[Columns.recvpath.ordinal()].getValue();
        this.sendPath = (String) allFields[Columns.sendpath.ordinal()].getValue();
        this.archivePath = (String) allFields[Columns.archivepath.ordinal()].getValue();
        this.workPath = (String) allFields[Columns.workpath.ordinal()].getValue();
        this.preTasks = (String) allFields[Columns.pretasks.ordinal()].getValue();
        this.postTasks = (String) allFields[Columns.posttasks.ordinal()].getValue();
        this.errorTasks = (String) allFields[Columns.errortasks.ordinal()].getValue();
        this.updatedInfo = (Integer) allFields[Columns.updatedinfo.ordinal()].getValue();
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
    public R66DbRule(String idRule, String ids, String recvPath,
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
    public R66DbRule(String idRule) throws OpenR66DatabaseException {
        this.idRule = idRule;
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
                    table+" WHERE "+Columns.idrule.name()+" = ?");
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
        R66DbPreparedStatement preparedStatement =
            new R66DbPreparedStatement(DbConstant.admin.session);
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
        R66DbPreparedStatement preparedStatement =
            new R66DbPreparedStatement(DbConstant.admin.session);
        try {
            preparedStatement.createPrepareStatement("SELECT "+selectAllFields+" FROM "+
                    table+" WHERE "+Columns.idrule.name()+" = ?");
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
        R66DbPreparedStatement preparedStatement =
            new R66DbPreparedStatement(DbConstant.admin.session);
        try {
            preparedStatement.createPrepareStatement("UPDATE "+table+" SET "+updateAllFields+
                    " WHERE "+Columns.idrule.name()+" = ?");
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

    public R66Rule getR66Rule() {
        return new R66Rule(this.idRule, this.ids, this.recvPath, this.sendPath,
                this.archivePath, this.workPath, this.preTasks, this.postTasks, this.errorTasks);
    }
}
