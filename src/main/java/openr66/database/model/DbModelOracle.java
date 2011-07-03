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
package openr66.database.model;

import goldengate.common.database.DbPreparedStatement;
import goldengate.common.database.DbRequest;
import goldengate.common.database.DbSession;
import goldengate.common.database.exception.GoldenGateDatabaseException;
import goldengate.common.database.exception.GoldenGateDatabaseNoConnectionError;
import goldengate.common.database.exception.GoldenGateDatabaseNoDataException;
import goldengate.common.database.exception.GoldenGateDatabaseSqlError;

import java.sql.SQLException;

import openr66.database.DbConstant;
import openr66.database.data.DbConfiguration;
import openr66.database.data.DbHostAuth;
import openr66.database.data.DbMultipleMonitor;
import openr66.database.data.DbRule;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.configuration.Configuration;

/**
 * Oracle Database Model implementation
 * @author Frederic Bregier
 *
 */
public class DbModelOracle extends goldengate.common.database.model.DbModelOracle {
    /**
     * Create the object and initialize if necessary the driver
     * @throws GoldenGateDatabaseNoConnectionError
     */
    public DbModelOracle() throws GoldenGateDatabaseNoConnectionError {
        super();
    }

    @Override
    public void createTables(DbSession session) throws GoldenGateDatabaseNoConnectionError {
        // Create tables: configuration, hosts, rules, runner, cptrunner
        String createTableH2 = "CREATE TABLE ";
        String constraint = " CONSTRAINT ";
        String primaryKey = " PRIMARY KEY ";
        String notNull = " NOT NULL ";

        // Multiple Mode
        String action = createTableH2 + DbMultipleMonitor.table + "(";
        DbMultipleMonitor.Columns[] mcolumns = DbMultipleMonitor.Columns
                .values();
        for (int i = 0; i < mcolumns.length - 1; i ++) {
            action += mcolumns[i].name() +
                    DBType.getType(DbMultipleMonitor.dbTypes[i]) + notNull +
                    ", ";
        }
        action += mcolumns[mcolumns.length - 1].name() +
                DBType.getType(DbMultipleMonitor.dbTypes[mcolumns.length - 1]) +
                notNull + ",";
        action += constraint+" conf_pk "+primaryKey+"("+mcolumns[mcolumns.length - 1].name()+"))";
        System.out.println(action);
        DbRequest request = new DbRequest(session);
        try {
            request.query(action);
        } catch (GoldenGateDatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (GoldenGateDatabaseSqlError e) {
            e.printStackTrace();
            return;
        } finally {
            request.close();
        }
        DbMultipleMonitor multipleMonitor = new DbMultipleMonitor(session, 
                Configuration.configuration.HOST_ID,0,0,0);
        try {
            multipleMonitor.insert();
        } catch (GoldenGateDatabaseException e1) {
            e1.printStackTrace();
        }

        // Configuration
        action = createTableH2 + DbConfiguration.table + "(";
        DbConfiguration.Columns[] ccolumns = DbConfiguration.Columns
                .values();
        for (int i = 0; i < ccolumns.length - 1; i ++) {
            action += ccolumns[i].name() +
                    DBType.getType(DbConfiguration.dbTypes[i]) + notNull +
                    ", ";
        }
        action += ccolumns[ccolumns.length - 1].name() +
                DBType.getType(DbConfiguration.dbTypes[ccolumns.length - 1]) +
                notNull + ",";
        action += constraint+" conf_pk "+primaryKey+"("+ccolumns[ccolumns.length - 1].name()+"))";
        System.out.println(action);
        request = new DbRequest(session);
        try {
            request.query(action);
        } catch (GoldenGateDatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (GoldenGateDatabaseSqlError e) {
            return;
        } finally {
            request.close();
        }

        // hosts
        action = createTableH2 + DbHostAuth.table + "(";
        DbHostAuth.Columns[] hcolumns = DbHostAuth.Columns.values();
        for (int i = 0; i < hcolumns.length - 1; i ++) {
            action += hcolumns[i].name() +
                    DBType.getType(DbHostAuth.dbTypes[i]) + notNull + ", ";
        }
        action += hcolumns[hcolumns.length - 1].name() +
                DBType.getType(DbHostAuth.dbTypes[hcolumns.length - 1]) +
                notNull + ",";
        action += constraint+" host_pk "+primaryKey+"("+hcolumns[hcolumns.length - 1].name()+"))";
        System.out.println(action);
        try {
            request.query(action);
        } catch (GoldenGateDatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (GoldenGateDatabaseSqlError e) {
            return;
        } finally {
            request.close();
        }

        // rules
        action = createTableH2 + DbRule.table + "(";
        DbRule.Columns[] rcolumns = DbRule.Columns.values();
        for (int i = 0; i < rcolumns.length - 1; i ++) {
            action += rcolumns[i].name() +
                    DBType.getType(DbRule.dbTypes[i]) + ", ";
        }
        action += rcolumns[rcolumns.length - 1].name() +
                DBType.getType(DbRule.dbTypes[rcolumns.length - 1]) +
                notNull + ",";
        action += constraint+" rule_pk "+primaryKey+"("+rcolumns[rcolumns.length - 1].name()+"))";
        System.out.println(action);
        try {
            request.query(action);
        } catch (GoldenGateDatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (GoldenGateDatabaseSqlError e) {
            return;
        } finally {
            request.close();
        }

        // runner
        action = createTableH2 + DbTaskRunner.table + "(";
        DbTaskRunner.Columns[] acolumns = DbTaskRunner.Columns.values();
        for (int i = 0; i < acolumns.length; i ++) {
            action += acolumns[i].name() +
                    DBType.getType(DbTaskRunner.dbTypes[i]) + notNull + ", ";
        }
        // Several columns for primary key
        action += constraint+" runner_pk " + primaryKey + "(";
        for (int i = DbTaskRunner.NBPRKEY; i > 1; i--) {
            action += acolumns[acolumns.length - i].name() + ",";
        }
        action += acolumns[acolumns.length - 1].name() + "))";
        System.out.println(action);
        try {
            request.query(action);
        } catch (GoldenGateDatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (GoldenGateDatabaseSqlError e) {
            return;
        } finally {
            request.close();
        }
        // Index Runner
        action = "CREATE INDEX IDX_RUNNER ON "+ DbTaskRunner.table + "(";
        DbTaskRunner.Columns[] icolumns = DbTaskRunner.indexes;
        for (int i = 0; i < icolumns.length-1; i ++) {
            action += icolumns[i].name()+ ", ";
        }
        action += icolumns[icolumns.length-1].name()+ ")";
        System.out.println(action);
        try {
            request.query(action);
        } catch (GoldenGateDatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (GoldenGateDatabaseSqlError e) {
            return;
        } finally {
            request.close();
        }

        // cptrunner
        action = "CREATE SEQUENCE " + DbTaskRunner.fieldseq +
                " MINVALUE " + (DbConstant.ILLEGALVALUE + 1)+
                " START WITH " + (DbConstant.ILLEGALVALUE + 1);
        System.out.println(action);
        try {
            request.query(action);
        } catch (GoldenGateDatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (GoldenGateDatabaseSqlError e) {
            return;
        } finally {
            request.close();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.databaseold.model.DbModel#resetSequence()
     */
    @Override
    public void resetSequence(DbSession session, long newvalue) throws GoldenGateDatabaseNoConnectionError {
        String action = "DROP SEQUENCE " + DbTaskRunner.fieldseq;
        String action2 = "CREATE SEQUENCE " + DbTaskRunner.fieldseq +
            " MINVALUE " + (DbConstant.ILLEGALVALUE + 1)+
            " START WITH " + (newvalue);
        DbRequest request = new DbRequest(session);
        try {
            request.query(action);
            request.query(action2);
        } catch (GoldenGateDatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (GoldenGateDatabaseSqlError e) {
            e.printStackTrace();
            return;
        } finally {
            request.close();
        }

        System.out.println(action);
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.databaseold.model.DbModel#nextSequence()
     */
    @Override
    public long nextSequence(DbSession dbSession)
        throws GoldenGateDatabaseNoConnectionError,
            GoldenGateDatabaseSqlError, GoldenGateDatabaseNoDataException {
        long result = DbConstant.ILLEGALVALUE;
        String action = "SELECT " + DbTaskRunner.fieldseq + ".NEXTVAL FROM DUAL";
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
                    throw new GoldenGateDatabaseSqlError(e);
                }
                return result;
            } else {
                throw new GoldenGateDatabaseNoDataException(
                        "No sequence found. Must be initialized first");
            }
        } finally {
            preparedStatement.realClose();
        }
    }
}
