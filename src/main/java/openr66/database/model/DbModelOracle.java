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
import goldengate.common.database.exception.OpenR66DatabaseException;
import goldengate.common.database.exception.OpenR66DatabaseNoConnectionError;
import goldengate.common.database.exception.OpenR66DatabaseNoDataException;
import goldengate.common.database.exception.OpenR66DatabaseSqlError;

import java.sql.SQLException;

import openr66.database.DbConstant;
import openr66.database.data.DbConfiguration;
import openr66.database.data.DbHostAuth;
import openr66.database.data.DbRule;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.utils.OpenR66SignalHandler;

/**
 * Oracle Database Model implementation
 * @author Frederic Bregier
 *
 */
public class DbModelOracle extends goldengate.common.database.model.DbModelOracle {
    /**
     * Create the object and initialize if necessary the driver
     * @throws OpenR66DatabaseNoConnectionError
     */
    public DbModelOracle() throws OpenR66DatabaseNoConnectionError {
        super();
    }

    @Override
    public void createTables(DbSession session) throws OpenR66DatabaseNoConnectionError {
        // Create tables: configuration, hosts, rules, runner, cptrunner
        String createTableH2 = "CREATE TABLE ";
        String constraint = " CONSTRAINT ";
        String primaryKey = " PRIMARY KEY ";
        String notNull = " NOT NULL ";

        // Configuration
        String action = createTableH2 + DbConfiguration.table + "(";
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
        DbRequest request = new DbRequest(session);
        try {
            request.query(action);
        } catch (OpenR66DatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (OpenR66DatabaseSqlError e) {
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
        } catch (OpenR66DatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (OpenR66DatabaseSqlError e) {
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
        } catch (OpenR66DatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (OpenR66DatabaseSqlError e) {
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
        } catch (OpenR66DatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (OpenR66DatabaseSqlError e) {
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
        } catch (OpenR66DatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (OpenR66DatabaseSqlError e) {
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
        } catch (OpenR66DatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (OpenR66DatabaseSqlError e) {
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
    public void resetSequence(DbSession session, long newvalue) throws OpenR66DatabaseNoConnectionError {
        String action = "DROP SEQUENCE " + DbTaskRunner.fieldseq;
        String action2 = "CREATE SEQUENCE " + DbTaskRunner.fieldseq +
            " MINVALUE " + (DbConstant.ILLEGALVALUE + 1)+
            " START WITH " + (newvalue);
        DbRequest request = new DbRequest(session);
        try {
            request.query(action);
            request.query(action2);
        } catch (OpenR66DatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (OpenR66DatabaseSqlError e) {
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
        throws OpenR66DatabaseNoConnectionError,
            OpenR66DatabaseSqlError, OpenR66DatabaseNoDataException {
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
                    throw new OpenR66DatabaseSqlError(e);
                }
                return result;
            } else {
                throw new OpenR66DatabaseNoDataException(
                        "No sequence found. Must be initialized first");
            }
        } finally {
            preparedStatement.realClose();
        }
    }

    /* (non-Javadoc)
     * @see openr66.databaseold.model.DbModel#validConnection(DbSession)
     */
    @Override
    public void validConnection(DbSession dbSession) throws OpenR66DatabaseNoConnectionError {
        DbRequest request = new DbRequest(dbSession, true);
        try {
            request.select("select 1 from dual");
            if (!request.getNext()) {
                throw new OpenR66DatabaseNoConnectionError(
                        "Cannot connect to database");
            }
        } catch (OpenR66DatabaseSqlError e) {
            try {
                DbSession newdbSession = new DbSession(dbSession.getAdmin(), false);
                try {
                    if (dbSession.conn != null) {
                        dbSession.conn.close();
                    }
                } catch (SQLException e1) {
                }
                dbSession.conn = newdbSession.conn;
                OpenR66SignalHandler.addConnection(dbSession.internalId, dbSession.conn);
                OpenR66SignalHandler.removeConnection(newdbSession.internalId);
                request.close();
                request.select("select 1 from dual");
                if (!request.getNext()) {
                    try {
                        if (dbSession.conn != null) {
                            dbSession.conn.close();
                        }
                    } catch (SQLException e1) {
                    }
                    OpenR66SignalHandler.removeConnection(dbSession.internalId);
                    throw new OpenR66DatabaseNoConnectionError(
                            "Cannot connect to database");
                }
                return;
            } catch (OpenR66DatabaseException e1) {
            }
            try {
                if (dbSession.conn != null) {
                    dbSession.conn.close();
                }
            } catch (SQLException e1) {
            }
            OpenR66SignalHandler.removeConnection(dbSession.internalId);
            throw new OpenR66DatabaseNoConnectionError(
                    "Cannot connect to database", e);
        } finally {
            request.close();
        }
    }

    /* (non-Javadoc)
     * @see openr66.databaseold.model.DbModel#limitRequest(java.lang.String, java.lang.String, int)
     */
    @Override
    public String limitRequest(String allfields, String request, int nb) {
        return "select "+allfields+" from ( "+request+" ) where rownum <= "+nb;
    }
}
