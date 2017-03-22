/**
 * This file is part of Waarp Project.
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All Waarp Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * 
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with Waarp . If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.database.data;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.waarp.common.database.DbPreparedStatement;
import org.waarp.common.database.DbSession;
import org.waarp.common.database.data.AbstractDbData;
import org.waarp.common.database.data.DbValue;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.database.exception.WaarpDatabaseNoDataException;
import org.waarp.common.database.exception.WaarpDatabaseSqlException;
import org.waarp.common.digest.FilesystemBasedDigest;
import org.waarp.common.json.JsonHandler;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.common.role.RoleDefault;
import org.waarp.common.utility.WaarpStringUtils;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessException;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Host Authentication Table object
 * 
 * @author Frederic Bregier
 * 
 */
public class DbHostAuth extends AbstractDbData {
    public static final String DEFAULT_CLIENT_ADDRESS = "0.0.0.0";

    /**
     * Internal Logger
     */
    private static final WaarpLogger logger = WaarpLoggerFactory
            .getLogger(DbHostAuth.class);

    public static enum Columns {
        ADDRESS, PORT, ISSSL, HOSTKEY, ADMINROLE, ISCLIENT, ISACTIVE, ISPROXIFIED, UPDATEDINFO, HOSTID
    }

    public static final int[] dbTypes = {
            Types.VARCHAR, Types.INTEGER, Types.BIT,
            Types.VARBINARY, Types.BIT, Types.BIT, Types.BIT, Types.BIT, Types.INTEGER, Types.NVARCHAR };

    public static final String table = " HOSTS ";

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

    private boolean isClient;

    private boolean isActive = true;

    private boolean isProxified = false;

    private int updatedInfo = UpdatedInfo.UNKNOWN
            .ordinal();

    // ALL TABLE SHOULD IMPLEMENT THIS
    public static final int NBPRKEY = 1;

    protected static final String selectAllFields = Columns.ADDRESS
            .name()
            + ","
            +
            Columns.PORT
                    .name()
            + ","
            + Columns.ISSSL
                    .name()
            + ","
            +
            Columns.HOSTKEY
                    .name()
            + ","
            +
            Columns.ADMINROLE
                    .name()
            + ","
            + Columns.ISCLIENT
                    .name()
            + ","
            + Columns.ISACTIVE
                    .name()
            + ","
            + Columns.ISPROXIFIED
                    .name()
            + ","
            +
            Columns.UPDATEDINFO
                    .name()
            + ","
            +
            Columns.HOSTID
                    .name();

    protected static final String updateAllFields =
            Columns.ADDRESS
                    .name()
                    + "=?,"
                    + Columns.PORT
                            .name()
                    +
                    "=?,"
                    + Columns.ISSSL
                            .name()
                    + "=?,"
                    + Columns.HOSTKEY
                            .name()
                    +
                    "=?,"
                    + Columns.ADMINROLE
                            .name()
                    + "=?,"
                    +
                    Columns.ISCLIENT
                            .name()
                    + "=?,"
                    +
                    Columns.ISACTIVE
                            .name()
                    + "=?,"
                    +
                    Columns.ISPROXIFIED
                            .name()
                    + "=?,"
                    +
                    Columns.UPDATEDINFO
                            .name()
                    + "=?";

    protected static final String insertAllValues = " (?,?,?,?,?,?,?,?,?,?) ";

    @Override
    protected void initObject() {
        primaryKey = new DbValue[] { new DbValue(hostid, Columns.HOSTID
                .name()) };
        otherFields = new DbValue[] {
                new DbValue(address, Columns.ADDRESS.name()),
                new DbValue(port, Columns.PORT.name()),
                new DbValue(isSsl, Columns.ISSSL.name()),
                new DbValue(hostkey, Columns.HOSTKEY.name()),
                new DbValue(adminrole, Columns.ADMINROLE.name()),
                new DbValue(isClient, Columns.ISCLIENT.name()),
                new DbValue(isActive, Columns.ISACTIVE.name()),
                new DbValue(isProxified, Columns.ISPROXIFIED.name()),
                new DbValue(updatedInfo, Columns.UPDATEDINFO.name()) };
        allFields = new DbValue[] {
                otherFields[0], otherFields[1], otherFields[2],
                otherFields[3], otherFields[4], otherFields[5], otherFields[6], otherFields[7], otherFields[8],
                primaryKey[0] };
    }

    @Override
    protected String getSelectAllFields() {
        return selectAllFields;
    }

    @Override
    protected String getTable() {
        return table;
    }

    @Override
    protected String getInsertAllValues() {
        return insertAllValues;
    }

    @Override
    protected String getUpdateAllFields() {
        return updateAllFields;
    }

    @Override
    protected void setToArray() {
        allFields[Columns.ADDRESS.ordinal()].setValue(address);
        allFields[Columns.PORT.ordinal()].setValue(port);
        allFields[Columns.ISSSL.ordinal()].setValue(isSsl);
        allFields[Columns.HOSTKEY.ordinal()].setValue(hostkey);
        allFields[Columns.ADMINROLE.ordinal()].setValue(adminrole);
        allFields[Columns.ISCLIENT.ordinal()].setValue(isClient);
        allFields[Columns.ISACTIVE.ordinal()].setValue(isActive);
        allFields[Columns.ISPROXIFIED.ordinal()].setValue(isProxified);
        allFields[Columns.UPDATEDINFO.ordinal()].setValue(updatedInfo);
        allFields[Columns.HOSTID.ordinal()].setValue(hostid);
    }

    @Override
    protected void setFromArray() throws WaarpDatabaseSqlException {
        address = (String) allFields[Columns.ADDRESS.ordinal()].getValue();
        port = (Integer) allFields[Columns.PORT.ordinal()].getValue();
        isSsl = (Boolean) allFields[Columns.ISSSL.ordinal()].getValue();
        hostkey = (byte[]) allFields[Columns.HOSTKEY.ordinal()].getValue();
        adminrole = (Boolean) allFields[Columns.ADMINROLE.ordinal()].getValue();
        isClient = (Boolean) allFields[Columns.ISCLIENT.ordinal()].getValue();
        isActive = (Boolean) allFields[Columns.ISACTIVE.ordinal()].getValue();
        isProxified = (Boolean) allFields[Columns.ISPROXIFIED.ordinal()].getValue();
        updatedInfo = (Integer) allFields[Columns.UPDATEDINFO.ordinal()]
                .getValue();
        hostid = (String) allFields[Columns.HOSTID.ordinal()].getValue();
    }

    @Override
    protected String getWherePrimaryKey() {
        return primaryKey[0].getColumn() + " = ? ";
    }

    @Override
    protected void setPrimaryKey() {
        primaryKey[0].setValue(hostid);
    }

    /**
     * @param dbSession
     * @param hostid
     * @param address
     * @param port
     * @param isSSL
     * @param hostkey
     * @param adminrole
     * @param isClient
     */
    public DbHostAuth(DbSession dbSession, String hostid, String address, int port,
            boolean isSSL, byte[] hostkey, boolean adminrole, boolean isClient) {
        super(dbSession);
        this.hostid = hostid;
        this.address = address;
        this.port = port;
        this.isSsl = isSSL;
        if (hostkey == null) {
            this.hostkey = null;
        } else {
            try {
                // Save as crypted with the local Key and HEX
                this.hostkey = Configuration.configuration.getCryptoKey().cryptToHex(hostkey)
                        .getBytes(WaarpStringUtils.UTF8);
            } catch (Exception e) {
                this.hostkey = new byte[0];
            }
        }
        this.adminrole = adminrole;
        this.isClient = isClient;
        if (this.port < 0) {
            this.isClient = true;
            this.address = DEFAULT_CLIENT_ADDRESS;
        }
        setToArray();
        isSaved = false;
    }

    public DbHostAuth(DbSession dbSession, ObjectNode source) throws WaarpDatabaseSqlException {
        super(dbSession);
        setFromJson(source, false);
        setToArray();
        isSaved = false;
    }

    @Override
    public void setFromJson(ObjectNode node, boolean ignorePrimaryKey) throws WaarpDatabaseSqlException {
        super.setFromJson(node, ignorePrimaryKey);
        if (hostkey == null || hostkey.length == 0 || address == null || address.isEmpty() || hostid == null
                || hostid.isEmpty()) {
            throw new WaarpDatabaseSqlException("Not enough argument to create the object");
        }
        if (hostkey != null) {
            try {
                // Save as crypted with the local Key and Base64
                this.hostkey = Configuration.configuration.getCryptoKey().cryptToHex(hostkey)
                        .getBytes(WaarpStringUtils.UTF8);
            } catch (Exception e) {
                this.hostkey = new byte[0];
            }
        }
        if (this.port < 0) {
            this.isClient = true;
            this.address = DEFAULT_CLIENT_ADDRESS;
        }
    }

    /**
     * @param dbSession
     * @param hostid
     * @throws WaarpDatabaseException
     */
    public DbHostAuth(DbSession dbSession, String hostid) throws WaarpDatabaseException {
        super(dbSession);
        if (hostid == null) {
            throw new WaarpDatabaseException("No host id passed");
        }
        this.hostid = hostid;
        if (Configuration.configuration.getAliases().containsKey(hostid)) {
            this.hostid = Configuration.configuration.getAliases().get(hostid);
        }
        // load from DB
        select();
    }

    /**
     * Delete all entries (used when purge and reload)
     * 
     * @param dbSession
     * @return the previous existing array of DbRule
     * @throws WaarpDatabaseException
     */
    public static DbHostAuth[] deleteAll(DbSession dbSession) throws WaarpDatabaseException {
        DbHostAuth[] result = getAllHosts(dbSession);
        if (dbSession == null) {
            dbR66HostAuthHashMap.clear();
            return result;
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("DELETE FROM " + table);
            preparedStatement.executeUpdate();
            return result;
        } finally {
            preparedStatement.realClose();
        }
    }

    @Override
    public void delete() throws WaarpDatabaseException {
        if (dbSession == null) {
            dbR66HostAuthHashMap.remove(this.hostid);
            isSaved = false;
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
                throw new WaarpDatabaseNoDataException("No row found");
            }
            isSaved = false;
        } finally {
            preparedStatement.realClose();
        }
    }

    @Override
    public void insert() throws WaarpDatabaseException {
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
                throw new WaarpDatabaseNoDataException("No row found");
            }
            isSaved = true;
        } finally {
            preparedStatement.realClose();
        }
    }

    @Override
    public boolean exist() throws WaarpDatabaseException {
        if (dbSession == null) {
            return dbR66HostAuthHashMap.containsKey(hostid);
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("SELECT " +
                    primaryKey[0].getColumn() + " FROM " + table + " WHERE " +
                    getWherePrimaryKey());
            setPrimaryKey();
            setValues(preparedStatement, primaryKey);
            preparedStatement.executeQuery();
            return preparedStatement.getNext();
        } finally {
            preparedStatement.realClose();
        }
    }

    @Override
    public void select() throws WaarpDatabaseException {
        if (dbSession == null) {
            DbHostAuth host = dbR66HostAuthHashMap.get(this.hostid);
            if (host == null) {
                throw new WaarpDatabaseNoDataException("No row found");
            } else {
                // copy info
                for (int i = 0; i < allFields.length; i++) {
                    allFields[i].setValue(host.allFields[i].getValue());
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
                    getWherePrimaryKey());
            setPrimaryKey();
            setValues(preparedStatement, primaryKey);
            preparedStatement.executeQuery();
            if (preparedStatement.getNext()) {
                getValues(preparedStatement, allFields);
                setFromArray();
                isSaved = true;
            } else {
                throw new WaarpDatabaseNoDataException("No row found");
            }
        } finally {
            preparedStatement.realClose();
        }
    }

    @Override
    public void update() throws WaarpDatabaseException {
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
                    getWherePrimaryKey());
            setValues(preparedStatement, allFields);
            int count = preparedStatement.executeUpdate();
            if (count <= 0) {
                throw new WaarpDatabaseNoDataException("No row found");
            }
            isSaved = true;
        } finally {
            preparedStatement.realClose();
        }
    }

    /**
     * Private constructor for Commander only
     */
    private DbHostAuth(DbSession session) {
        super(session);
    }

    /**
     * Get All DbHostAuth from database or from internal hashMap in case of no database support
     * 
     * @param dbSession
     *            may be null
     * @return the array of DbHostAuth
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     */
    public static DbHostAuth[] getAllHosts(DbSession dbSession)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        if (dbSession == null) {
            DbHostAuth[] result = new DbHostAuth[0];
            return dbR66HostAuthHashMap.values().toArray(result);
        }
        String request = "SELECT " + selectAllFields;
        request += " FROM " + table;
        ArrayList<DbHostAuth> dbArrayList = new ArrayList<DbHostAuth>();
        DbPreparedStatement preparedStatement = new DbPreparedStatement(dbSession, request);
        try {
            preparedStatement.executeQuery();
            while (preparedStatement.getNext()) {
                DbHostAuth hostAuth = getFromStatement(preparedStatement);
                dbArrayList.add(hostAuth);
            }
        } finally {
            preparedStatement.realClose();
        }
        DbHostAuth[] result = new DbHostAuth[0];
        return dbArrayList.toArray(result);
    }

    /**
     * For instance from Commander when getting updated information
     * 
     * @param preparedStatement
     * @return the next updated DbHostAuth
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     */
    public static DbHostAuth getFromStatement(DbPreparedStatement preparedStatement)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        DbHostAuth dbHostAuth = new DbHostAuth(preparedStatement.getDbSession());
        dbHostAuth.getValues(preparedStatement, dbHostAuth.allFields);
        dbHostAuth.setFromArray();
        dbHostAuth.isSaved = true;
        return dbHostAuth;
    }

    /**
     * 
     * @return the DbPreparedStatement for getting Updated Object
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     */
    public static DbPreparedStatement getUpdatedPrepareStament(DbSession session)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        String request = "SELECT " + selectAllFields;
        request += " FROM " + table +
                " WHERE " + Columns.UPDATEDINFO.name() + " = " +
                AbstractDbData.UpdatedInfo.TOSUBMIT.ordinal();
        DbPreparedStatement prep = new DbPreparedStatement(session, request);
        return prep;
    }

    /**
     * 
     * @param session
     * @param host
     * @param addr
     * @param ssl
     * @param active
     * @return the DbPreparedStatement according to the filter
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     */
    public static DbPreparedStatement getFilterPrepareStament(DbSession session,
            String host, String addr, boolean ssl, boolean active)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        DbPreparedStatement preparedStatement = new DbPreparedStatement(session);
        String request = "SELECT " + selectAllFields + " FROM " + table + " WHERE ";
        String condition = null;
        if (host != null) {
            condition = Columns.HOSTID.name() + " LIKE '%" + host + "%' ";
        }
        if (addr != null) {
            if (condition != null) {
                condition += " AND ";
            } else {
                condition = "";
            }
            condition += Columns.ADDRESS.name() + " LIKE '%" + addr + "%' ";
        }
        if (condition != null) {
            condition += " AND ";
        } else {
            condition = "";
        }
        condition += Columns.ISSSL.name() + " = ? AND ";
        condition += Columns.ISACTIVE.name() + " = ? ";
        preparedStatement.createPrepareStatement(request + condition +
                " ORDER BY " + Columns.HOSTID.name());
        try {
            preparedStatement.getPreparedStatement().setBoolean(1, ssl);
            preparedStatement.getPreparedStatement().setBoolean(2, active);
        } catch (SQLException e) {
            preparedStatement.realClose();
            throw new WaarpDatabaseSqlException(e);
        }
        return preparedStatement;
    }

    /**
     * 
     * @param session
     * @param host
     * @param addr
     * @return the DbPreparedStatement according to the filter
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     */
    public static DbPreparedStatement getFilterPrepareStament(DbSession session,
            String host, String addr)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        DbPreparedStatement preparedStatement = new DbPreparedStatement(session);
        String request = "SELECT " + selectAllFields + " FROM " + table;
        String condition = null;
        if (host != null) {
            condition = Columns.HOSTID.name() + " LIKE '%" + host + "%' ";
        }
        if (addr != null) {
            if (condition != null) {
                condition += " AND ";
            } else {
                condition = "";
            }
            condition += Columns.ADDRESS.name() + " LIKE '%" + addr + "%' ";
        }
        if (condition != null) {
            condition = " WHERE " + condition;
        } else {
            condition = "";
        }
        preparedStatement.createPrepareStatement(request + condition +
                " ORDER BY " + Columns.HOSTID.name());
        return preparedStatement;
    }

    @Override
    public void changeUpdatedInfo(UpdatedInfo info) {
        if (updatedInfo != info.ordinal()) {
            updatedInfo = info.ordinal();
            allFields[Columns.UPDATEDINFO.ordinal()].setValue(updatedInfo);
            isSaved = false;
        }
    }

    /**
     * @return the isActive
     */
    public boolean isActive() {
        return isActive;
    }

    /**
     * @param isActive
     *            the isActive to set
     */
    public void setActive(boolean isActive) {
        this.isActive = isActive;
        allFields[Columns.ISACTIVE.ordinal()].setValue(this.isActive);
        isSaved = false;
    }

    /**
     * @return the isProxified
     */
    public boolean isProxified() {
        return isProxified;
    }

    /**
     * @param isProxified
     *            the isProxified to set
     */
    public void setProxified(boolean isProxified) {
        this.isProxified = isProxified;
        allFields[Columns.ISPROXIFIED.ordinal()].setValue(this.isProxified);
        isSaved = false;
        if (this.isProxified) {
            Configuration.configuration.setBlacklistBadAuthent(false);
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
        // Check before if any key is passed or if account is active
        if (newkey == null || !isActive) {
            return false;
        }
        try {
            return FilesystemBasedDigest.equalPasswd(
                    Configuration.configuration.getCryptoKey().decryptHexInBytes(this.hostkey),
                    newkey);
        } catch (Exception e) {
            logger.debug("Error while checking key", e);
            return false;
        }
    }

    /**
     * @return the hostkey
     */
    public byte[] getHostkey() {
        if (hostkey == null) {
            return null;
        }
        try {
            return Configuration.configuration.getCryptoKey().decryptHexInBytes(hostkey);
        } catch (Exception e) {
            return new byte[0];
        }
    }

    /**
     * @return the adminrole
     */
    public boolean isAdminrole() {
        return adminrole;
    }

    /**
     * Test if the address is 0.0.0.0 for a client or isClient
     * 
     * @return True if the address is a client address (0.0.0.0) or isClient
     */
    public boolean isClient() {
        return isClient || isNoAddress();
    }

    /**
     * True if the address is a client address (0.0.0.0) or if the port is < 0
     * 
     * @return True if the address is a client address (0.0.0.0) or if the port is < 0
     */
    public boolean isNoAddress() {
        return (this.address.equals(DEFAULT_CLIENT_ADDRESS) || this.port < 0);
    }

    /**
     * 
     * @return the SocketAddress from the address and port
     * @exception IllegalArgumentException
     *                when the address is for a Client and therefore cannot be checked
     */
    public SocketAddress getSocketAddress() throws IllegalArgumentException {
        if (isNoAddress()) {
            throw new IllegalArgumentException("Not a server");
        }
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

    private static String getVersion(String host) {
        String remoteHost = host;
        String alias = "";
        if (Configuration.configuration.getAliases().containsKey(remoteHost)) {
            remoteHost = Configuration.configuration.getAliases().get(remoteHost);
            alias += "(Alias: " + remoteHost + ") ";
        }
        if (Configuration.configuration.getReverseAliases().containsKey(remoteHost)) {
            String alias2 = "(ReverseAlias: ";
            String[] list = Configuration.configuration.getReverseAliases().get(remoteHost);
            boolean found = false;
            for (String string : list) {
                if (string.equals(host)) {
                    continue;
                }
                found = true;
                alias2 += string + " ";
            }
            if (found) {
                alias += alias2 + ") ";
            }
        }
        if (Configuration.configuration.getBusinessWhiteSet().contains(remoteHost)) {
            alias += "(Business: Allowed) ";
        }
        if (Configuration.configuration.getRoles().containsKey(remoteHost)) {
            RoleDefault item = Configuration.configuration.getRoles().get(remoteHost);
            alias += "(Role: " + item.toString() + ") ";
        }
        return alias + (Configuration.configuration.getVersions().containsKey(remoteHost) ?
                Configuration.configuration.getVersions().get(remoteHost).toString() :
                "Version Unknown");
    }

    @Override
    public String toString() {
        //System.err.println(hostid+" Version: "+Configuration.configuration.versions.get(hostid)+":"+Configuration.configuration.versions.containsKey(hostid));
        return "HostAuth: " + hostid + " address: " + address + ":" + port + " isSSL: " + isSsl +
                " admin: " + adminrole + " isClient: " + isClient + " isActive: " + isActive
                + " isProxified: " + isProxified + " ("
                + (hostkey != null ? hostkey.length : 0) + ") Version: "
                + getVersion(hostid);
    }

    /**
     * Write selected DbHostAuth to a Json String
     * 
     * @param preparedStatement
     * @return the associated Json String
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     * @throws OpenR66ProtocolBusinessException
     */
    public static String getJson(DbPreparedStatement preparedStatement, int limit)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException,
            OpenR66ProtocolBusinessException {
        ArrayNode arrayNode = JsonHandler.createArrayNode();
        try {
            preparedStatement.executeQuery();
            int nb = 0;
            while (preparedStatement.getNext()) {
                DbHostAuth host = DbHostAuth
                        .getFromStatement(preparedStatement);
                ObjectNode node = host.getInternalJson();
                arrayNode.add(node);
                nb++;
                if (nb >= limit) {
                    break;
                }
            }
        } finally {
            preparedStatement.realClose();
        }
        return JsonHandler.writeAsString(arrayNode);
    }
    private ObjectNode getInternalJson() {
        ObjectNode node = getJson();
        try {
            node.put(Columns.HOSTKEY.name(), Configuration.configuration.getCryptoKey().decryptHexInString(
                    new String(hostkey, WaarpStringUtils.UTF8)));
        } catch (Exception e1) {
            node.put(Columns.HOSTKEY.name(), "");
        }
        int nb = 0;
        try {
            nb = NetworkTransaction.nbAttachedConnection(getSocketAddress(), getHostid());
        } catch (Exception e) {
            nb = -1;
        }
        node.put("Connection", nb); 
        node.put("Version", getVersion(hostid).replace("\"", "").replace(",", ", "));
        return node;
    }
    /**
     * 
     * @return the Json string for this
     */
    public String getJsonAsString() {
        ObjectNode node = getInternalJson();
        return JsonHandler.writeAsString(node);
    }
    /**
     * @param session
     * @param body
     * @param crypted
     *            True if the Key is kept crypted, False it will be in clear form
     * @return the runner in Html format specified by body by replacing all instance of fields
     */
    public String toSpecializedHtml(R66Session session, String body, boolean crypted) {
        StringBuilder builder = new StringBuilder(body);
        WaarpStringUtils.replace(builder, "XXXHOSTXXX", hostid);
        WaarpStringUtils.replace(builder, "XXXADDRXXX", address);
        WaarpStringUtils.replace(builder, "XXXPORTXXX", Integer.toString(port));
        if (crypted) {
            WaarpStringUtils.replace(builder, "XXXKEYXXX", new String(hostkey, WaarpStringUtils.UTF8));
        } else {
            try {
                WaarpStringUtils.replace(builder, "XXXKEYXXX",
                        Configuration.configuration.getCryptoKey().decryptHexInString(new String(
                                this.hostkey, WaarpStringUtils.UTF8)));
            } catch (Exception e) {
                WaarpStringUtils.replace(builder, "XXXKEYXXX", "BAD DECRYPT");
            }
        }
        WaarpStringUtils.replace(builder, "XXXSSLXXX", isSsl ? "checked" : "");
        WaarpStringUtils.replace(builder, "XXXADMXXX", adminrole ? "checked" : "");
        WaarpStringUtils.replace(builder, "XXXISCXXX", isClient ? "checked" : "");
        WaarpStringUtils.replace(builder, "XXXISAXXX", isActive ? "checked" : "");
        WaarpStringUtils.replace(builder, "XXXISPXXX", isProxified ? "checked" : "");
        WaarpStringUtils.replace(builder, "XXXVERSIONXXX", getVersion(hostid).replace(",", ", "));
        int nb = 0;
        try {
            nb = NetworkTransaction.nbAttachedConnection(getSocketAddress(), getHostid());
        } catch (Exception e) {
            nb = -1;
        }
        WaarpStringUtils.replace(builder, "XXXCONNXXX", (nb > 0)
                ? "(" + nb + " Connected) " : "");
        return builder.toString();
    }

    /**
     * 
     * @param session
     * @return True if any of the server has the isProxified property
     */
    public static boolean hasProxifiedHosts(DbSession session) {
        if (session == null) {
            for (DbHostAuth host : dbR66HostAuthHashMap.values()) {
                if (host.isProxified) {
                    return true;
                }
            }
            return false;
        }
        DbPreparedStatement preparedStatement = null;
        int val = 0;
        try {
            preparedStatement = new DbPreparedStatement(session,
                    "SELECT count(*) FROM " + table + " WHERE " + Columns.ISPROXIFIED + " = " + true);
            preparedStatement.executeQuery();
            preparedStatement.getNext();
            val = preparedStatement.getResultSet().getInt(1);
        } catch (WaarpDatabaseNoConnectionException e) {
            return false;
        } catch (WaarpDatabaseSqlException e) {
            return false;
        } catch (SQLException e) {
            return false;
        } finally {
            if (preparedStatement != null) {
                preparedStatement.realClose();
            }
        }
        return val > 0;
    }

    /**
     * 
     * @return the DbValue associated with this table
     */
    public static DbValue[] getAllType() {
        DbHostAuth item = new DbHostAuth(null);
        return item.allFields;
    }
}
