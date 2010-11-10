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
package openr66.database.model;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;

import openr66.database.DbConstant;
import openr66.database.DbPreparedStatement;
import openr66.database.DbRequest;
import openr66.database.DbSession;
import openr66.database.data.DbConfiguration;
import openr66.database.data.DbHostAuth;
import openr66.database.data.DbRule;
import openr66.database.data.DbTaskRunner;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseNoDataException;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.protocol.utils.OpenR66SignalHandler;

/**
 * H2 Database Model implementation
 * @author Frederic Bregier
 *
 */
public class DbModelH2 implements DbModel {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(DbModelH2.class);

    public static DbType type = DbType.H2;
    /**
     * Create the object and initialize if necessary the driver
     * @throws OpenR66DatabaseNoConnectionError
     */
    public DbModelH2() throws OpenR66DatabaseNoConnectionError {
        if (DbModelFactory.classLoaded) {
            return;
        }
        try {
            DriverManager.registerDriver(new org.h2.Driver());
            DbModelFactory.classLoaded = true;
        } catch (SQLException e) {
         // SQLException
            logger.error("Cannot register Driver " + type.name()+ "\n"+e.getMessage());
            DbSession.error(e);
            throw new OpenR66DatabaseNoConnectionError(
                    "Cannot load database drive:" + type.name(), e);
        }

    }

    private static enum DBType {
        CHAR(Types.CHAR, " CHAR(3) "),
        VARCHAR(Types.VARCHAR, " VARCHAR(254) "),
        LONGVARCHAR(Types.LONGVARCHAR, " LONGVARCHAR "),
        BIT(Types.BIT, " BOOLEAN "),
        TINYINT(Types.TINYINT, " TINYINT "),
        SMALLINT(Types.SMALLINT, " SMALLINT "),
        INTEGER(Types.INTEGER, " INTEGER "),
        BIGINT(Types.BIGINT, " BIGINT "),
        REAL(Types.REAL, " REAL "),
        DOUBLE(Types.DOUBLE, " DOUBLE "),
        VARBINARY(Types.VARBINARY, " BINARY "),
        DATE(Types.DATE, " DATE "),
        TIMESTAMP(Types.TIMESTAMP, " TIMESTAMP ");

        public int type;

        public String name;

        public String constructor;

        private DBType(int type, String constructor) {
            this.type = type;
            name = name();
            this.constructor = constructor;
        }

        public static String getType(int sqltype) {
            switch (sqltype) {
                case Types.CHAR:
                    return CHAR.constructor;
                case Types.VARCHAR:
                    return VARCHAR.constructor;
                case Types.LONGVARCHAR:
                    return LONGVARCHAR.constructor;
                case Types.BIT:
                    return BIT.constructor;
                case Types.TINYINT:
                    return TINYINT.constructor;
                case Types.SMALLINT:
                    return SMALLINT.constructor;
                case Types.INTEGER:
                    return INTEGER.constructor;
                case Types.BIGINT:
                    return BIGINT.constructor;
                case Types.REAL:
                    return REAL.constructor;
                case Types.DOUBLE:
                    return DOUBLE.constructor;
                case Types.VARBINARY:
                    return VARBINARY.constructor;
                case Types.DATE:
                    return DATE.constructor;
                case Types.TIMESTAMP:
                    return TIMESTAMP.constructor;
                default:
                    return null;
            }
        }
    }

    @Override
    public void createTables() throws OpenR66DatabaseNoConnectionError {
        // Create tables: configuration, hosts, rules, runner, cptrunner
        String createTableH2 = "CREATE TABLE IF NOT EXISTS ";
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
                primaryKey + ")";
        System.out.println(action);
        DbRequest request = new DbRequest(DbConstant.admin.session);
        try {
            request.query(action);
        } catch (OpenR66DatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (OpenR66DatabaseSqlError e) {
            e.printStackTrace();
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
                primaryKey + ")";
        System.out.println(action);
        try {
            request.query(action);
        } catch (OpenR66DatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (OpenR66DatabaseSqlError e) {
            e.printStackTrace();
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
                primaryKey + ")";
        System.out.println(action);
        try {
            request.query(action);
        } catch (OpenR66DatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (OpenR66DatabaseSqlError e) {
            e.printStackTrace();
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
        action += " CONSTRAINT runner_pk " + primaryKey + "(";
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
            e.printStackTrace();
            return;
        } finally {
            request.close();
        }
        // Index Runner
        action = "CREATE INDEX IF NOT EXISTS IDX_RUNNER ON "+ DbTaskRunner.table + "(";
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
        action = "CREATE SEQUENCE IF NOT EXISTS " + DbTaskRunner.fieldseq +
                " START WITH " + (DbConstant.ILLEGALVALUE + 1);
        System.out.println(action);
        try {
            request.query(action);
        } catch (OpenR66DatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (OpenR66DatabaseSqlError e) {
            e.printStackTrace();
            return;
        } finally {
            request.close();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.model.DbModel#resetSequence()
     */
    @Override
    public void resetSequence(long newvalue) throws OpenR66DatabaseNoConnectionError {
        String action = "ALTER SEQUENCE " + DbTaskRunner.fieldseq +
                " RESTART WITH " + newvalue;
        DbRequest request = new DbRequest(DbConstant.admin.session);
        try {
            request.query(action);
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
     * @see openr66.database.model.DbModel#nextSequence()
     */
    @Override
    public long nextSequence(DbSession dbSession)
        throws OpenR66DatabaseNoConnectionError,
            OpenR66DatabaseSqlError, OpenR66DatabaseNoDataException {
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
     * @see openr66.database.model.DbModel#validConnection(DbSession)
     */
    @Override
    public void validConnection(DbSession dbSession) throws OpenR66DatabaseNoConnectionError {
        DbRequest request = new DbRequest(dbSession, true);
        try {
            request.select("select 1");
            if (!request.getNext()) {
                throw new OpenR66DatabaseNoConnectionError(
                        "Cannot connect to database");
            }
        } catch (OpenR66DatabaseSqlError e) {
            try {
                DbSession newdbSession = new DbSession(DbConstant.admin, false);
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
                request.select("select 1");
                if (!request.getNext()) {
                    try {
                        if (dbSession.conn != null) {
                            dbSession.conn.close();
                        }
                    } catch (SQLException e1) {
                    }
                    OpenR66SignalHandler.removeConnection(dbSession.internalId);                    throw new OpenR66DatabaseNoConnectionError(
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
    @Override
    public String limitRequest(String allfields, String request, int nb) {
        return request+" LIMIT "+nb;
    }

}
