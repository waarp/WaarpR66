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
package org.waarp.openr66.database.model;

import java.sql.SQLException;

import org.waarp.common.database.DbPreparedStatement;
import org.waarp.common.database.DbRequest;
import org.waarp.common.database.DbSession;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.database.exception.WaarpDatabaseNoDataException;
import org.waarp.common.database.exception.WaarpDatabaseSqlException;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbConfiguration;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.database.data.DbHostConfiguration;
import org.waarp.openr66.database.data.DbMultipleMonitor;
import org.waarp.openr66.database.data.DbRule;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.PartnerConfiguration;
import org.waarp.openr66.protocol.utils.R66Versions;

/**
 * PostGreSQL Database Model implementation
 * 
 * @author Frederic Bregier
 * 
 */
public class DbModelPostgresql extends org.waarp.common.database.model.DbModelPostgresql {
    /**
     * Create the object and initialize if necessary the driver
     * 
     * @throws WaarpDatabaseNoConnectionException
     */
    public DbModelPostgresql() throws WaarpDatabaseNoConnectionException {
        super();
    }

    /**
     * Gets the version of the current PostgreSQL Server
     *
     * @throws WaarpDatabaseNoConnectionException
     */
    private int getServerVersion(DbSession session) throws WaarpDatabaseNoConnectionException {
        int serverVersion = 0;
        DbRequest request = new DbRequest(session);
        try {
            request.select("SHOW server_version_num");
            request.getNext();
            serverVersion = request.getResultSet().getInt("server_version_num");
        } catch (WaarpDatabaseNoConnectionException e) {
            e.printStackTrace();
        } catch (WaarpDatabaseSqlException e) {
            e.printStackTrace();
            // XXX FIX no return;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            request.close();
        }
        return serverVersion;
    }

    @Override
    public void createTables(DbSession session) throws WaarpDatabaseNoConnectionException {
        // Create tables: configuration, hosts, rules, runner, cptrunner
        String createTableH2 = "CREATE TABLE ";
        String primaryKey = " PRIMARY KEY ";
        String notNull = " NOT NULL ";

        // Multiple Mode
        String action = createTableH2 + DbMultipleMonitor.table + "(";
        DbMultipleMonitor.Columns[] mcolumns = DbMultipleMonitor.Columns
                .values();
        for (int i = 0; i < mcolumns.length - 1; i++) {
            action += mcolumns[i].name() +
                    DBType.getType(DbMultipleMonitor.dbTypes[i]) + notNull +
                    ", ";
        }
        action += mcolumns[mcolumns.length - 1].name() +
                DBType.getType(DbMultipleMonitor.dbTypes[mcolumns.length - 1]) +
                primaryKey + ")";
        System.out.println(action);
        DbRequest request = new DbRequest(session);
        try {
            request.query(action);
        } catch (WaarpDatabaseNoConnectionException e) {
            e.printStackTrace();
            return;
        } catch (WaarpDatabaseSqlException e) {
            e.printStackTrace();
            // XXX FIX no return;
        } finally {
            request.close();
        }
        DbMultipleMonitor multipleMonitor = new DbMultipleMonitor(session,
                Configuration.configuration.getHOST_ID(), 0, 0, 0);
        try {
            if (!multipleMonitor.exist())
                multipleMonitor.insert();
        } catch (WaarpDatabaseException e1) {
            e1.printStackTrace();
        }

        // Configuration
        action = createTableH2 + DbConfiguration.table + "(";
        DbConfiguration.Columns[] ccolumns = DbConfiguration.Columns
                .values();
        for (int i = 0; i < ccolumns.length - 1; i++) {
            action += ccolumns[i].name() +
                    DBType.getType(DbConfiguration.dbTypes[i]) + notNull +
                    ", ";
        }
        action += ccolumns[ccolumns.length - 1].name() +
                DBType.getType(DbConfiguration.dbTypes[ccolumns.length - 1]) +
                primaryKey + ")";
        System.out.println(action);
        request = new DbRequest(session);
        try {
            request.query(action);
        } catch (WaarpDatabaseNoConnectionException e) {
            e.printStackTrace();
            return;
        } catch (WaarpDatabaseSqlException e) {
            e.printStackTrace();
            // XXX FIX no return;
        } finally {
            request.close();
        }

        // HostConfiguration
        action = createTableH2 + DbHostConfiguration.table + "(";
        DbHostConfiguration.Columns[] chcolumns = DbHostConfiguration.Columns
                .values();
        for (int i = 0; i < chcolumns.length - 1; i++) {
            action += chcolumns[i].name() +
                    DBType.getType(DbHostConfiguration.dbTypes[i]) + notNull +
                    ", ";
        }
        action += chcolumns[chcolumns.length - 1].name() +
                DBType.getType(DbHostConfiguration.dbTypes[chcolumns.length - 1]) +
                primaryKey + ")";
        System.out.println(action);
        request = new DbRequest(session);
        try {
            request.query(action);
        } catch (WaarpDatabaseNoConnectionException e) {
            e.printStackTrace();
            return;
        } catch (WaarpDatabaseSqlException e) {
            // XXX FIX no return;
        } finally {
            request.close();
        }

        // hosts
        action = createTableH2 + DbHostAuth.table + "(";
        DbHostAuth.Columns[] hcolumns = DbHostAuth.Columns.values();
        for (int i = 0; i < hcolumns.length - 1; i++) {
            action += hcolumns[i].name() +
                    DBType.getType(DbHostAuth.dbTypes[i]) + notNull + ", ";
        }
        action += hcolumns[hcolumns.length - 1].name() +
                DBType.getType(DbHostAuth.dbTypes[hcolumns.length - 1]) +
                primaryKey + ")";
        System.out.println(action);
        try {
            request.query(action);
        } catch (WaarpDatabaseNoConnectionException e) {
            e.printStackTrace();
            return;
        } catch (WaarpDatabaseSqlException e) {
            e.printStackTrace();
            // XXX FIX no return;
        } finally {
            request.close();
        }

        // rules
        action = createTableH2 + DbRule.table + "(";
        DbRule.Columns[] rcolumns = DbRule.Columns.values();
        for (int i = 0; i < rcolumns.length - 1; i++) {
            action += rcolumns[i].name() +
                    DBType.getType(DbRule.dbTypes[i]) + ", ";
        }
        action += rcolumns[rcolumns.length - 1].name() +
                DBType.getType(DbRule.dbTypes[rcolumns.length - 1]) +
                primaryKey + ")";
        System.out.println(action);
        try {
            request.query(action);
        } catch (WaarpDatabaseNoConnectionException e) {
            e.printStackTrace();
            return;
        } catch (WaarpDatabaseSqlException e) {
            e.printStackTrace();
            // XXX FIX no return;
        } finally {
            request.close();
        }

        // runner
        action = createTableH2 + DbTaskRunner.table + "(";
        DbTaskRunner.Columns[] acolumns = DbTaskRunner.Columns.values();
        for (int i = 0; i < acolumns.length; i++) {
            action += acolumns[i].name() +
                    DBType.getType(DbTaskRunner.dbTypes[i]) + notNull + ", ";
        }
        // Several columns for primary key
        action += " CONSTRAINT runner_pk " + primaryKey + "(";
        for (int i = DbTaskRunner.NBPRKEY; i > 1; i--) {
            action += acolumns[acolumns.length - i].name() + ",";
        }
        action += acolumns[acolumns.length - 1].name() + "))";
        System.out.println(action);
        try {
            request.query(action);
        } catch (WaarpDatabaseNoConnectionException e) {
            e.printStackTrace();
            return;
        } catch (WaarpDatabaseSqlException e) {
            e.printStackTrace();
            // XXX FIX no return;
        } finally {
            request.close();
        }
        // Index Runner
        action = "CREATE INDEX IDX_RUNNER ON " + DbTaskRunner.table + "(";
        DbTaskRunner.Columns[] icolumns = DbTaskRunner.indexes;
        for (int i = 0; i < icolumns.length - 1; i++) {
            action += icolumns[i].name() + ", ";
        }
        action += icolumns[icolumns.length - 1].name() + ")";
        System.out.println(action);
        try {
            request.query(action);
        } catch (WaarpDatabaseNoConnectionException e) {
            e.printStackTrace();
            return;
        } catch (WaarpDatabaseSqlException e) {
            // XXX FIX no return;
        } finally {
            request.close();
        }

        // cptrunner
        action = "CREATE SEQUENCE " + DbTaskRunner.fieldseq +
                " MINVALUE " + (DbConstant.ILLEGALVALUE + 1) +
                " START WITH " + (DbConstant.ILLEGALVALUE + 1);
        System.out.println(action);
        try {
            request.query(action);
        } catch (WaarpDatabaseNoConnectionException e) {
            e.printStackTrace();
            return;
        } catch (WaarpDatabaseSqlException e) {
            e.printStackTrace();
            // XXX FIX no return;
        } finally {
            request.close();
        }

        DbHostConfiguration.updateVersionDb(session, Configuration.configuration.getHOST_ID(),
            R66Versions.V2_4_25.getVersion());
    }

    @Override
    public void resetSequence(DbSession session, long newvalue)
            throws WaarpDatabaseNoConnectionException {
        String action = "ALTER SEQUENCE " + DbTaskRunner.fieldseq +
                " MINVALUE " + (DbConstant.ILLEGALVALUE + 1) +
                " RESTART WITH " + newvalue;
        DbRequest request = new DbRequest(session);
        try {
            request.query(action);
        } catch (WaarpDatabaseNoConnectionException e) {
            e.printStackTrace();
            return;
        } catch (WaarpDatabaseSqlException e) {
            e.printStackTrace();
            return;
        } finally {
            request.close();
        }
        System.out.println(action);
    }

    @Override
    public long nextSequence(DbSession dbSession)
            throws WaarpDatabaseNoConnectionException,
            WaarpDatabaseSqlException, WaarpDatabaseNoDataException {
        long result = DbConstant.ILLEGALVALUE;
        String action = "SELECT NEXTVAL('" + DbTaskRunner.fieldseq + "')";
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement(action);
            // Limit the search
            preparedStatement.executeQuery();
            if (preparedStatement.getNext()) {
                try {
                    result = preparedStatement.getResultSet().getLong(1);
                } catch (SQLException e) {
                    throw new WaarpDatabaseSqlException(e);
                }
                return result;
            } else {
                throw new WaarpDatabaseNoDataException(
                        "No sequence found. Must be initialized first");
            }
        } finally {
            preparedStatement.realClose();
        }
    }

    public boolean upgradeDb(DbSession session, String version) throws WaarpDatabaseNoConnectionException {
        if (PartnerConfiguration.isVersion2GEQVersion1(version, R66Versions.V2_4_13.getVersion())) {
            System.out.println(version + " to " + R66Versions.V2_4_13.getVersion() + "? " + true);

            String createTableH2 = "CREATE TABLE ";
            int serverVersion = getServerVersion(session);
            if (serverVersion >= 90100) {
                createTableH2 = "CREATE TABLE IF NOT EXISTS ";
            }
            String primaryKey = " PRIMARY KEY ";
            String notNull = " NOT NULL ";

            // HostConfiguration
            String action = createTableH2 + DbHostConfiguration.table + "(";
            DbHostConfiguration.Columns[] chcolumns = DbHostConfiguration.Columns
                    .values();
            for (int i = 0; i < chcolumns.length - 1; i++) {
                action += chcolumns[i].name() +
                        DBType.getType(DbHostConfiguration.dbTypes[i]) + notNull +
                        ", ";
            }
            action += chcolumns[chcolumns.length - 1].name() +
                    DBType.getType(DbHostConfiguration.dbTypes[chcolumns.length - 1]) +
                    primaryKey + ")";
            System.out.println(action);
            DbRequest request = new DbRequest(session);
            try {
                request.query(action);
            } catch (WaarpDatabaseSqlException e) {
                e.printStackTrace();
                return false;
            } finally {
                request.close();
            }
        }
        if (PartnerConfiguration.isVersion2GEQVersion1(version, R66Versions.V2_4_17.getVersion())) {
            System.out.println(version + " to " + R66Versions.V2_4_17.getVersion() + "? " + true);
            String command = "DO $$ BEGIN " +
                    "ALTER TABLE " + DbTaskRunner.table + " ADD COLUMN " +
                    DbTaskRunner.Columns.TRANSFERINFO.name() + " " +
                    DBType.getType(DbTaskRunner.dbTypes[DbTaskRunner.Columns.TRANSFERINFO.ordinal()]) +
                    " DEFAULT '{}' NOT NULL; " +
                    "EXCEPTION WHEN duplicate_column THEN END $$";
            DbRequest request = new DbRequest(session);
            try {
                request.query(command);
            } catch (WaarpDatabaseSqlException e) {
                e.printStackTrace();
                //return false;
            } finally {
                request.close();
            }
        }
        if (PartnerConfiguration.isVersion2GEQVersion1(version, R66Versions.V2_4_23.getVersion())) {
            System.out.println(version + " to " + R66Versions.V2_4_23.getVersion() + "? " + true);
            String command = "DO $$ BEGIN " +
                    "ALTER TABLE " + DbHostAuth.table + " ADD COLUMN " +
                    DbHostAuth.Columns.ISACTIVE.name() + " " +
                    DBType.getType(DbHostAuth.dbTypes[DbHostAuth.Columns.ISACTIVE.ordinal()]) +
                    " DEFAULT " + true + " NOT NULL; " +
                    "EXCEPTION WHEN duplicate_column THEN END $$";
            DbRequest request = new DbRequest(session);
            try {
                request.query(command);
            } catch (WaarpDatabaseSqlException e) {
                e.printStackTrace();
                //return false;
            } finally {
                request.close();
            }
            command = "DO $$ BEGIN " +
                    "ALTER TABLE " + DbHostAuth.table + " ADD COLUMN " +
                    DbHostAuth.Columns.ISPROXIFIED.name() + " " +
                    DBType.getType(DbHostAuth.dbTypes[DbHostAuth.Columns.ISPROXIFIED.ordinal()]) +
                    " DEFAULT " + false + " NOT NULL; " +
                    "EXCEPTION WHEN duplicate_column THEN END $$";
            request = new DbRequest(session);
            try {
                request.query(command);
            } catch (WaarpDatabaseSqlException e) {
                e.printStackTrace();
                //return false;
            } finally {
                request.close();
            }
        }
        if (PartnerConfiguration.isVersion2GTVersion1(version, R66Versions.V2_4_25.getVersion())) {
            System.out.println(version + " to " + R66Versions.V2_4_25.getVersion() + "? " + true);
            String command = "ALTER TABLE " + DbTaskRunner.table + " ALTER COLUMN " +
                    DbTaskRunner.Columns.FILENAME.name() + " TYPE " +
                    DBType.getType(DbTaskRunner.dbTypes[DbTaskRunner.Columns.FILENAME.ordinal()]) + "," +
                    " ALTER COLUMN " + DbTaskRunner.Columns.FILENAME.name() + " SET NOT NULL, " +
                    " ALTER COLUMN " + DbTaskRunner.Columns.ORIGINALNAME.name() + " TYPE " +
                    DBType.getType(DbTaskRunner.dbTypes[DbTaskRunner.Columns.ORIGINALNAME.ordinal()]) + "," +
                    " ALTER COLUMN " + DbTaskRunner.Columns.FILENAME.name() + " SET NOT NULL ";
            DbRequest request = new DbRequest(session);
            try {
                System.out.println("Command: " + command);
                request.query(command);
            } catch (WaarpDatabaseSqlException e) {
                e.printStackTrace();
                return false;
            } finally {
                request.close();
            }
        }
        DbHostConfiguration.updateVersionDb(session, Configuration.configuration.getHOST_ID(),
                R66Versions.V2_4_25.getVersion());
        return true;
    }

    public boolean needUpgradeDb(DbSession session, String version, boolean tryFix)
            throws WaarpDatabaseNoConnectionException {
        // Check if the database is up to date
        DbRequest request = null;
        if (PartnerConfiguration.isVersion2GEQVersion1(version, R66Versions.V2_4_13.getVersion())) {
            try {
                request = new DbRequest(session);
                request.select("select " + DbHostConfiguration.Columns.HOSTID.name() + " from "
                        + DbHostConfiguration.table +
                        " where " + DbHostConfiguration.Columns.HOSTID + " = '" + Configuration.configuration.getHOST_ID()
                        + "'");
                request.close();
                DbHostConfiguration.updateVersionDb(session, Configuration.configuration.getHOST_ID(),
                        R66Versions.V2_4_13.getVersion());
            } catch (WaarpDatabaseSqlException e) {
                return !upgradeDb(session, version);
            } finally {
                if (request != null) {
                    request.close();
                }
            }
        }
        request = null;
        if (PartnerConfiguration.isVersion2GEQVersion1(version, R66Versions.V2_4_17.getVersion())) {
            try {
                request = new DbRequest(session);
                request.select("select " + DbTaskRunner.Columns.TRANSFERINFO.name() + " from " + DbTaskRunner.table +
                        " where " + DbTaskRunner.Columns.SPECIALID + " = " + DbConstant.ILLEGALVALUE);
                request.close();
                DbHostConfiguration.updateVersionDb(session, Configuration.configuration.getHOST_ID(),
                        R66Versions.V2_4_17.getVersion());
            } catch (WaarpDatabaseSqlException e) {
                return !upgradeDb(session, version);
            } finally {
                if (request != null) {
                    request.close();
                }
            }
        }
        request = null;
        if (PartnerConfiguration.isVersion2GEQVersion1(version, R66Versions.V2_4_23.getVersion())) {
            try {
                request = new DbRequest(session);
                request.select("select " + DbHostAuth.Columns.ISACTIVE.name() + " from " + DbHostAuth.table +
                        " where " + DbHostAuth.Columns.PORT + " = " + 0);
                request.close();
                DbHostConfiguration.updateVersionDb(session, Configuration.configuration.getHOST_ID(),
                        R66Versions.V2_4_23.getVersion());
            } catch (WaarpDatabaseSqlException e) {
                return !upgradeDb(session, version);
            } finally {
                if (request != null) {
                    request.close();
                }
            }
        }
        request = null;
        if (PartnerConfiguration.isVersion2GTVersion1(version, R66Versions.V2_4_25.getVersion())) {
            try {
                if (upgradeDb(session, version)) {
                    DbHostConfiguration.updateVersionDb(session, Configuration.configuration.getHOST_ID(),
                            R66Versions.V2_4_25.getVersion());
                } else {
                    return true;
                }
            } finally {
            }
        }
        return false;
    }

}
