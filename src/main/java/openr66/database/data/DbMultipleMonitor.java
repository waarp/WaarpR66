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

import goldengate.common.database.DbPreparedStatement;
import goldengate.common.database.DbSession;
import goldengate.common.database.data.AbstractDbData;
import goldengate.common.database.data.DbValue;
import goldengate.common.database.exception.GoldenGateDatabaseException;
import goldengate.common.database.exception.GoldenGateDatabaseNoConnectionError;
import goldengate.common.database.exception.GoldenGateDatabaseNoDataException;
import goldengate.common.database.exception.GoldenGateDatabaseSqlError;

import java.sql.Types;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Configuration Table object
 *
 * @author Frederic Bregier
 *
 */
public class DbMultipleMonitor extends AbstractDbData {
    public static enum Columns {
        COUNTCONFIG,
        COUNTHOST,
        COUNTRULE,
        HOSTID
    }

    public static final int[] dbTypes = {
        Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR };

    public static final String table = " MULTIPLEMONITOR ";

    /**
     * HashTable in case of lack of database
     */
    private static final ConcurrentHashMap<String, DbMultipleMonitor> dbR66MMHashMap =
        new ConcurrentHashMap<String, DbMultipleMonitor>();

    private String hostid;

    public int countConfig;

    public int countHost;

    public int countRule;

    // ALL TABLE SHOULD IMPLEMENT THIS
    public static final int NBPRKEY = 1;

    protected static final String selectAllFields = Columns.COUNTCONFIG
            .name() +
            "," +
            Columns.COUNTHOST.name() +
            "," +
            Columns.COUNTRULE.name() +
            "," +
            Columns.HOSTID.name();

    protected static final String updateAllFields = Columns.COUNTCONFIG
            .name() +
            "=?," +
            Columns.COUNTHOST.name() +
            "=?," +
            Columns.COUNTRULE.name() +
            "=?";

    protected static final String insertAllValues = " (?,?,?,?) ";


    /* (non-Javadoc)
     * @see goldengate.common.database.data.AbstractDbData#initObject()
     */
    @Override
    protected void initObject() {
        primaryKey = new DbValue[]{new DbValue(hostid, Columns.HOSTID
                .name())};
        otherFields = new DbValue[]{
                new DbValue(countConfig, Columns.COUNTCONFIG.name()),
                new DbValue(countHost, Columns.COUNTHOST.name()),
                new DbValue(countRule, Columns.COUNTRULE.name()) };
        allFields = new DbValue[]{
                otherFields[0], otherFields[1], otherFields[2], primaryKey[0] };
    }

    /* (non-Javadoc)
     * @see goldengate.common.database.data.AbstractDbData#getSelectAllFields()
     */
    @Override
    protected String getSelectAllFields() {
        return selectAllFields;
    }

    /* (non-Javadoc)
     * @see goldengate.common.database.data.AbstractDbData#getTable()
     */
    @Override
    protected String getTable() {
        return table;
    }

    /* (non-Javadoc)
     * @see goldengate.common.database.data.AbstractDbData#getInsertAllValues()
     */
    @Override
    protected String getInsertAllValues() {
        return insertAllValues;
    }

    /* (non-Javadoc)
     * @see goldengate.common.database.data.AbstractDbData#getUpdateAllFields()
     */
    @Override
    protected String getUpdateAllFields() {
        return updateAllFields;
    }

    @Override
    protected void setToArray() {
        allFields[Columns.HOSTID.ordinal()].setValue(hostid);
        allFields[Columns.COUNTCONFIG.ordinal()].setValue(countConfig);
        allFields[Columns.COUNTHOST.ordinal()]
                .setValue(countHost);
        allFields[Columns.COUNTRULE.ordinal()]
                .setValue(countRule);
    }

    @Override
    protected void setFromArray() throws GoldenGateDatabaseSqlError {
        hostid = (String) allFields[Columns.HOSTID.ordinal()].getValue();
        countConfig = (Integer) allFields[Columns.COUNTCONFIG.ordinal()]
                .getValue();
        countHost = (Integer) allFields[Columns.COUNTHOST.ordinal()]
                .getValue();
        countRule = (Integer) allFields[Columns.COUNTRULE.ordinal()]
                .getValue();
    }

    /* (non-Javadoc)
     * @see goldengate.common.database.data.AbstractDbData#getWherePrimaryKey()
     */
    @Override
    protected String getWherePrimaryKey() {
        return primaryKey[0].column + " = ? ";
    }

    /* (non-Javadoc)
     * @see goldengate.common.database.data.AbstractDbData#setPrimaryKey()
     */
    @Override
    protected void setPrimaryKey() {
        primaryKey[0].setValue(hostid);
    }

    /**
     * @param dbSession
     * @param hostid
     * @param cc
     *            count for Config
     * @param ch
     *            count for Host
     * @param cr
     *            count for Rule
     */
    public DbMultipleMonitor(DbSession dbSession, String hostid, int cc, int ch, int cr) {
        super(dbSession);
        this.hostid = hostid;
        countConfig = cc;
        countHost = ch;
        countRule = cr;
        setToArray();
        isSaved = false;
    }

    /**
     * @param dbSession
     * @param hostid
     * @throws GoldenGateDatabaseException
     */
    public DbMultipleMonitor(DbSession dbSession, String hostid) throws GoldenGateDatabaseException {
        super(dbSession);
        this.hostid = hostid;
        // load from DB
        select();
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.databaseold.data.AbstractDbData#delete()
     */
    @Override
    public void delete() throws GoldenGateDatabaseException {
        if (dbSession == null) {
            dbR66MMHashMap.remove(this.hostid);
            isSaved = false;
            return;
        }
        super.delete();
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.databaseold.data.AbstractDbData#insert()
     */
    @Override
    public void insert() throws GoldenGateDatabaseException {
        if (isSaved) {
            return;
        }
        if (dbSession == null) {
            dbR66MMHashMap.put(this.hostid, this);
            isSaved = true;
            return;
        }
        super.insert();
    }

    /* (non-Javadoc)
     * @see openr66.databaseold.data.AbstractDbData#exist()
     */
    @Override
    public boolean exist() throws GoldenGateDatabaseException {
        if (dbSession == null) {
            return dbR66MMHashMap.containsKey(hostid);
        }
        return super.exist();
    }
    /*
     * (non-Javadoc)
     *
     * @see openr66.databaseold.data.AbstractDbData#select()
     */
    @Override
    public void select() throws GoldenGateDatabaseException {
        if (dbSession == null) {
            DbMultipleMonitor conf = dbR66MMHashMap.get(this.hostid);
            if (conf == null) {
                throw new GoldenGateDatabaseNoDataException("No row found");
            } else {
                // copy info
                for (int i = 0; i < allFields.length; i++){
                    allFields[i].value = conf.allFields[i].value;
                }
                setFromArray();
                isSaved = true;
                return;
            }
        }
        super.select();
    }
    /*
     * (non-Javadoc)
     *
     * @see openr66.databaseold.data.AbstractDbData#update()
     */
    @Override
    public void update() throws GoldenGateDatabaseException {
        if (isSaved) {
            return;
        }
        if (dbSession == null) {
            dbR66MMHashMap.put(this.hostid, this);
            isSaved = true;
            return;
        }
        super.update();
    }
    /**
     * Private constructor for Commander only
     */
    private DbMultipleMonitor(DbSession session) {
        super(session);
    }
    /**
     * For instance from Commander when getting updated information
     * @param preparedStatement
     * @return the next updated Configuration
     * @throws GoldenGateDatabaseNoConnectionError
     * @throws GoldenGateDatabaseSqlError
     */
    public static DbMultipleMonitor getFromStatement(DbPreparedStatement preparedStatement) throws GoldenGateDatabaseNoConnectionError, GoldenGateDatabaseSqlError {
        DbMultipleMonitor dbMm = new DbMultipleMonitor(preparedStatement.getDbSession());
        dbMm.getValues(preparedStatement, dbMm.allFields);
        dbMm.setFromArray();
        dbMm.isSaved = true;
        return dbMm;
    }
    /**
     *
     * @return the DbPreparedStatement for getting Updated Object
     * @throws GoldenGateDatabaseNoConnectionError
     * @throws GoldenGateDatabaseSqlError
     */
    public static DbPreparedStatement getUpdatedPrepareStament(DbSession session) throws GoldenGateDatabaseNoConnectionError, GoldenGateDatabaseSqlError {
        String request = "SELECT " +selectAllFields;
        request += " FROM "+table+
            " FOR UPDATE ";
        DbPreparedStatement prep = new DbPreparedStatement(session, request);
        session.addLongTermPreparedStatement(prep);
        return prep;
    }
    /*
     * (non-Javadoc)
     *
     * @see openr66.databaseold.data.AbstractDbData#changeUpdatedInfo(UpdatedInfo)
     */
    @Override
    public void changeUpdatedInfo(UpdatedInfo info) {
    }
}
