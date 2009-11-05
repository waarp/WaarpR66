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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import openr66.context.R66Session;
import openr66.database.DbConstant;
import openr66.database.DbPreparedStatement;
import openr66.database.DbSession;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseNoDataException;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.FileUtils;

/**
 * Host Authentication Table object
 *
 * @author Frederic Bregier
 *
 */
public class DbHostAuth extends AbstractDbData {
    public static enum Columns {
        ADDRESS, PORT, SSL, HOSTKEY, ADMINROLE, UPDATEDINFO, HOSTID
    }

    public static int[] dbTypes = {
        Types.VARCHAR, Types.INTEGER, Types.BIT,
        Types.VARBINARY, Types.BIT, Types.INTEGER, Types.VARCHAR };

    public static String table = " HOSTS ";

    /**
     * HashTable in case of lack of database
     */
    private static final ConcurrentHashMap<String, DbHostAuth> dbR66HostAuthHashMap =
        new ConcurrentHashMap<String, DbHostAuth>();

    private String hostid;

    private String address;

    private int port;

    private boolean isSsl;

    private byte[] hostkey;

    private boolean adminrole;

    private int updatedInfo = UpdatedInfo.UNKNOWN.ordinal();

    private boolean isSaved = false;

    // ALL TABLE SHOULD IMPLEMENT THIS
    private final DbValue primaryKey = new DbValue(hostid, Columns.HOSTID
            .name());

    private final DbValue[] otherFields = {
            new DbValue(address, Columns.ADDRESS.name()),
            new DbValue(port, Columns.PORT.name()),
            new DbValue(isSsl, Columns.SSL.name()),
            new DbValue(hostkey, Columns.HOSTKEY.name()),
            new DbValue(adminrole, Columns.ADMINROLE.name()),
            new DbValue(updatedInfo, Columns.UPDATEDINFO.name()) };

    private final DbValue[] allFields = {
            otherFields[0], otherFields[1], otherFields[2],
            otherFields[3], otherFields[4], otherFields[5], primaryKey };

    public static final String selectAllFields = Columns.ADDRESS.name() + "," +
            Columns.PORT.name() + "," +Columns.SSL.name() + "," +
            Columns.HOSTKEY.name() + "," +
            Columns.ADMINROLE.name() + "," + Columns.UPDATEDINFO.name() + "," +
            Columns.HOSTID.name();

    private static final String updateAllFields =
        Columns.ADDRESS.name() + "=?," +Columns.PORT.name() +
        "=?," +Columns.SSL.name() + "=?," + Columns.HOSTKEY.name() +
            "=?," + Columns.ADMINROLE.name() + "=?," +
            Columns.UPDATEDINFO.name() + "=?";

    private static final String insertAllValues = " (?,?,?,?,?,?,?) ";

    @Override
    protected void setToArray() {
        allFields[Columns.ADDRESS.ordinal()].setValue(address);
        allFields[Columns.PORT.ordinal()].setValue(port);
        allFields[Columns.SSL.ordinal()].setValue(isSsl);
        allFields[Columns.HOSTKEY.ordinal()].setValue(hostkey);
        allFields[Columns.ADMINROLE.ordinal()].setValue(adminrole);
        allFields[Columns.UPDATEDINFO.ordinal()].setValue(updatedInfo);
        allFields[Columns.HOSTID.ordinal()].setValue(hostid);
    }

    @Override
    protected void setFromArray() throws OpenR66DatabaseSqlError {
        address = (String) allFields[Columns.ADDRESS.ordinal()].getValue();
        port = (Integer) allFields[Columns.PORT.ordinal()].getValue();
        isSsl = (Boolean) allFields[Columns.SSL.ordinal()].getValue();
        hostkey = (byte[]) allFields[Columns.HOSTKEY.ordinal()].getValue();
        adminrole = (Boolean) allFields[Columns.ADMINROLE.ordinal()].getValue();
        updatedInfo = (Integer) allFields[Columns.UPDATEDINFO.ordinal()]
                .getValue();
        hostid = (String) allFields[Columns.HOSTID.ordinal()].getValue();
    }

    /**
     * @param dbSession
     * @param hostid
     * @param address
     * @param port
     * @param isSSL
     * @param hostkey
     * @param adminrole
     */
    public DbHostAuth(DbSession dbSession, String hostid, String address, int port,
            boolean isSSL, byte[] hostkey, boolean adminrole) {
        super(dbSession);
        this.hostid = hostid;
        this.address = address;
        this.port = port;
        this.isSsl = isSSL;
        this.hostkey = hostkey;
        this.adminrole = adminrole;
        setToArray();
        isSaved = false;
    }

    /**
     * @param dbSession
     * @param hostid
     * @throws OpenR66DatabaseException
     */
    public DbHostAuth(DbSession dbSession, String hostid) throws OpenR66DatabaseException {
        super(dbSession);
        this.hostid = hostid;
        // load from DB
        select();
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.data.AbstractDbData#delete()
     */
    @Override
    public void delete() throws OpenR66DatabaseException {
        if (dbSession == null) {
            dbR66HostAuthHashMap.remove(this.hostid);
            isSaved = false;
            return;
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("DELETE FROM " + table +
                    " WHERE " + primaryKey.column + " = ?");
            primaryKey.setValue(hostid);
            setValue(preparedStatement, primaryKey);
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
            dbR66HostAuthHashMap.put(this.hostid, this);
            isSaved = true;
            return;
        }
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

    /* (non-Javadoc)
     * @see openr66.database.data.AbstractDbData#exist()
     */
    @Override
    public boolean exist() throws OpenR66DatabaseException {
        if (dbSession == null) {
            return dbR66HostAuthHashMap.containsKey(hostid);
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("SELECT " +
                    primaryKey.column + " FROM " + table + " WHERE " +
                    primaryKey.column + " = ?");
            primaryKey.setValue(hostid);
            setValue(preparedStatement, primaryKey);
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
            DbHostAuth host = dbR66HostAuthHashMap.get(this.hostid);
            if (host == null) {
                throw new OpenR66DatabaseNoDataException("No row found");
            } else {
                // copy info
                for (int i = 0; i < allFields.length; i++){
                    allFields[i].value = host.allFields[i].value;
                }
                setFromArray();
                isSaved = true;
                return;
            }
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("SELECT " +
                    selectAllFields + " FROM " + table + " WHERE " +
                    primaryKey.column + " = ?");
            primaryKey.setValue(hostid);
            setValue(preparedStatement, primaryKey);
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
        if (dbSession == null) {
            dbR66HostAuthHashMap.put(this.hostid, this);
            isSaved = true;
            return;
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("UPDATE " + table +
                    " SET " + updateAllFields + " WHERE " +
                    primaryKey.column + " = ?");
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
     */
    private DbHostAuth() {
        super(DbConstant.admin.session);
    }
    /**
     * For instance from Commander when getting updated information
     * @param preparedStatement
     * @return the next updated DbHostAuth
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public static DbHostAuth getFromStatement(DbPreparedStatement preparedStatement) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        DbHostAuth dbHostAuth = new DbHostAuth();
        dbHostAuth.getValues(preparedStatement, dbHostAuth.allFields);
        dbHostAuth.setFromArray();
        dbHostAuth.isSaved = true;
        return dbHostAuth;
    }
    /**
    *
    * @return the DbPreparedStatement for getting Updated Object
    * @throws OpenR66DatabaseNoConnectionError
    * @throws OpenR66DatabaseSqlError
    */
   public static DbPreparedStatement getUpdatedPrepareStament(DbSession session) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
       String request = "SELECT " +selectAllFields;
       request += " FROM "+table+
           " WHERE "+Columns.UPDATEDINFO.name()+" = "+
           AbstractDbData.UpdatedInfo.TOSUBMIT.ordinal();
       return new DbPreparedStatement(session, request);
   }
   /**
    *
    * @param session
    * @param host
    * @param addr
    * @param ssl
    * @return the DbPreparedStatement according to the filter
    * @throws OpenR66DatabaseNoConnectionError
    * @throws OpenR66DatabaseSqlError
    */
   public static DbPreparedStatement getFilterPrepareStament(DbSession session,
           String host, String addr, boolean ssl)
       throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
       DbPreparedStatement preparedStatement = new DbPreparedStatement(session);
       String request = "SELECT " +selectAllFields+" FROM "+table+" WHERE ";
       String condition = null;
       if (host != null) {
           condition = Columns.HOSTID.name()+" LIKE '%"+host+"%' ";
       }
       if (addr != null) {
           if (condition != null) {
               condition += " AND ";
           } else {
               condition = "";
           }
           condition += Columns.ADDRESS.name()+" LIKE '%"+addr+"%' ";
       }
       if (condition != null) {
           condition += " AND ";
       } else {
           condition = "";
       }
       condition += Columns.SSL.name()+" = ?";
       preparedStatement.createPrepareStatement(request+condition+
               " ORDER BY "+Columns.HOSTID.name());
       try {
           preparedStatement.getPreparedStatement().setBoolean(1, ssl);
       } catch (SQLException e) {
           preparedStatement.realClose();
           throw new OpenR66DatabaseSqlError(e);
       }
       return preparedStatement;
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
     * Is the given key a valid one
     *
     * @param newkey
     * @return True if the key is valid (or any key is valid)
     */
    public boolean isKeyValid(byte[] newkey) {
        // It is valid to not have a key
        if (this.hostkey == null) {
            return true;
        }
        if (newkey == null) {
            return false;
        }
        return Arrays.equals(this.hostkey, newkey);
    }

    /**
     * @return the hostkey
     */
    public byte[] getHostkey() {
        return hostkey;
    }

    /**
     * @return the adminrole
     */
    public boolean isAdminrole() {
        return adminrole;
    }
    /**
     *
     * @return the SocketAddress from the address and port
     */
    public SocketAddress getSocketAddress() {
        return new InetSocketAddress(this.address, this.port);
    }
    /**
     *
     * @return True if this Host ref is with SSL support
     */
    public boolean isSsl() {
        return this.isSsl;
    }

    /**
     * @return the hostid
     */
    public String getHostid() {
        return hostid;
    }

    /**
     * @return the address
     */
    public String getAddress() {
        return address;
    }

    /**
     * @return the port
     */
    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "HostAuth: " + hostid + " address: " +address+":"+port+" isSSL: "+isSsl+
        " admin: "+ adminrole +" "+(hostkey!=null?hostkey.length:0);
    }
    /**
     * @param session
     * @param body
     * @return the runner in Html format specified by body by replacing all instance of fields
     */
    public String toSpecializedHtml(R66Session session, String body) {
        StringBuilder builder = new StringBuilder(body);
        FileUtils.replace(builder, "XXXHOSTXXX", hostid);
        FileUtils.replace(builder, "XXXADDRXXX", address);
        FileUtils.replace(builder, "XXXPORTXXX", Integer.toString(port));
        FileUtils.replace(builder, "XXXKEYXXX", new String(hostkey));
        FileUtils.replace(builder, "XXXSSLXXX", isSsl ? "checked": "");
        FileUtils.replace(builder, "XXXADMXXX", adminrole ? "checked": "");
        int nb = NetworkTransaction.existConnection(getSocketAddress(), getHostid());
        FileUtils.replace(builder, "XXXCONNXXX", (nb > 0)
                ? "("+nb+" Connected) ": "");
        return builder.toString();
    }
}
