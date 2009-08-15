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
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseNoDataException;
import openr66.database.exception.OpenR66DatabaseSqlError;

/**
 * Oracle Database Model implementation
 * @author Frederic Bregier
 *
 */
public class DbModelOracle extends AbstractDbModel {
    private static enum DBType {
        CHAR(Types.CHAR, " CHAR(3) "),
        VARCHAR(Types.VARCHAR, " VARCHAR2(254) "),
        LONGVARCHAR(Types.LONGVARCHAR, " CLOB "),
        BIT(Types.BIT, " BOOLEAN "),
        TINYINT(Types.TINYINT, " SMALLINT "),
        SMALLINT(Types.SMALLINT, " SMALLINT "),
        INTEGER(Types.INTEGER, " INTEGER "),
        BIGINT(Types.BIGINT, " NUMBER(38,0) "),
        REAL(Types.REAL, " REAL "),
        DOUBLE(Types.DOUBLE, " DOUBLE PRECISION "),
        VARBINARY(Types.VARBINARY, " BLOB "),
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
    public void createTables() {
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
        DbRequest request = new DbRequest(DbConstant.admin.session);
        try {
            request.query(action);
        } catch (OpenR66DatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (OpenR66DatabaseSqlError e) {
            e.printStackTrace();
            return;
        }
        request.close();

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
            e.printStackTrace();
            return;
        }
        request.close();

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
            e.printStackTrace();
            return;
        }
        request.close();

        // runner
        action = createTableH2 + DbTaskRunner.table + "(";
        DbTaskRunner.Columns[] acolumns = DbTaskRunner.Columns.values();
        for (int i = 0; i < acolumns.length; i ++) {
            action += acolumns[i].name() +
                    DBType.getType(DbTaskRunner.dbTypes[i]) + notNull + ", ";
        }
        // Two columns for primary key
        action += constraint+" runner_pk " + primaryKey + "(" +
                acolumns[acolumns.length - 3].name() + "," +
                acolumns[acolumns.length - 2].name() + "," +
                acolumns[acolumns.length - 1].name() + "))";
        System.out.println(action);
        try {
            request.query(action);
        } catch (OpenR66DatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (OpenR66DatabaseSqlError e) {
            e.printStackTrace();
            return;
        }
        request.close();

        // cptrunner
        action = "CREATE SEQUENCE " + DbTaskRunner.fieldseq +
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
        }
        request.close();
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.model.AbstractDbModel#resetSequence()
     */
    @Override
    public void resetSequence(long newvalue) {
        String action = "ALTER SEQUENCE " + DbTaskRunner.fieldseq +
                " MINVALUE " + newvalue;
        DbRequest request = new DbRequest(DbConstant.admin.session);
        try {
            request.query(action);
        } catch (OpenR66DatabaseNoConnectionError e) {
            e.printStackTrace();
            return;
        } catch (OpenR66DatabaseSqlError e) {
            e.printStackTrace();
            return;
        }
        request.close();
        System.out.println(action);
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.model.AbstractDbModel#nextSequence()
     */
    @Override
    public long nextSequence(DbSession dbSession)
        throws OpenR66DatabaseNoConnectionError,
            OpenR66DatabaseSqlError, OpenR66DatabaseNoDataException {
        long result = DbConstant.ILLEGALVALUE;
        String action = "SELECT NEXTVAL " + DbTaskRunner.fieldseq + " FROM DUAL";
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        preparedStatement.createPrepareStatement(action);
        try {
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
}
