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

import openr66.context.authentication.R66SimpleAuth;
import openr66.database.DbConstant;
import openr66.database.DbPreparedStatement;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoDataException;
import openr66.database.exception.OpenR66DatabaseSqlError;

/**
 * @author Frederic Bregier
 *
 */
public class DbR66HostAuth extends AbstractDbData  {
    public static enum Columns {
        HOSTKEY, ADMINROLE,
        UPDATEDINFO,
        HOSTID
    }
    public static int [] dbTypes = {
        Types.VARBINARY, Types.BIT,
        Types.INTEGER, Types.VARCHAR
    };
    public static String table = " HOSTS ";


    private String hostid;

    private byte[] hostkey;

    private boolean adminrole;

    private int updatedInfo;

    private boolean isSaved = false;

    // ALL TABLE SHOULD IMPLEMENT THIS
    private final DbValue primaryKey = new DbValue(hostid, Columns.HOSTID.name());
    private final DbValue[] otherFields = {
      new DbValue(hostkey, Columns.HOSTKEY.name()),
      new DbValue(adminrole, Columns.ADMINROLE.name()),
      new DbValue(updatedInfo, Columns.UPDATEDINFO.name())
    };
    private final DbValue[] allFields = {
      otherFields[0], otherFields[1], otherFields[2], primaryKey
    };
    private static final String selectAllFields =
        Columns.HOSTKEY.name()+","+Columns.ADMINROLE.name()+
        ","+Columns.UPDATEDINFO.name()+
        ","+Columns.HOSTID.name();
    private static final String updateAllFields =
        Columns.HOSTKEY.name()+"=?,"+Columns.ADMINROLE.name()+
        "=?,"+Columns.UPDATEDINFO.name()+"=?";
    private static final String insertAllValues = " (?,?,?,?) ";

    @Override
    protected void setToArray() {
        allFields[Columns.HOSTID.ordinal()].setValue(this.hostid);
        allFields[Columns.HOSTKEY.ordinal()].setValue(this.hostkey);
        allFields[Columns.ADMINROLE.ordinal()].setValue(this.adminrole);
        allFields[Columns.UPDATEDINFO.ordinal()].setValue(this.updatedInfo);
    }
    @Override
    protected void setFromArray() throws OpenR66DatabaseSqlError {
        this.hostid = (String) allFields[Columns.HOSTID.ordinal()].getValue();
        this.hostkey = (byte[]) allFields[Columns.HOSTKEY.ordinal()].getValue();
        this.adminrole = (Boolean) allFields[Columns.ADMINROLE.ordinal()].getValue();
        this.updatedInfo = (Integer) allFields[Columns.UPDATEDINFO.ordinal()].getValue();
    }


    /**
     * @param hostid
     * @param hostkey
     * @param adminrole
     * @param updatedInfo
     */
    public DbR66HostAuth(String hostid, byte[] hostkey, boolean adminrole, int updatedInfo) {
        this.hostid = hostid;
        this.hostkey = hostkey;
        this.adminrole = adminrole;
        this.updatedInfo = updatedInfo;
        this.setToArray();
        this.isSaved = false;
    }


    /**
     * @param hostid
     * @throws OpenR66DatabaseException
     */
    public DbR66HostAuth(String hostid) throws OpenR66DatabaseException {
        this.hostid = hostid;
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
                    table+" WHERE "+Columns.HOSTID.name()+" = ?");
            primaryKey.setValue(hostid);
            this.setValue(preparedStatement, primaryKey);
            int count = preparedStatement.executeUpdate();
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
                    table+" WHERE "+Columns.HOSTID.name()+" = ?");
            primaryKey.setValue(hostid);
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
        DbPreparedStatement preparedStatement =
            new DbPreparedStatement(DbConstant.admin.session);
        try {
            preparedStatement.createPrepareStatement("UPDATE "+table+" SET "+updateAllFields+
                    " WHERE "+Columns.HOSTID.name()+" = ?");
            this.setValues(preparedStatement, allFields);
            int count = preparedStatement.executeUpdate();
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

    public R66SimpleAuth getR66SimpleAuth() {
        R66SimpleAuth auth = new R66SimpleAuth(this.hostid, this.hostkey);
        auth.setAdmin(this.adminrole);
        return auth;
    }
}
