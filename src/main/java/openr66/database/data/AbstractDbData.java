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

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import openr66.database.DbPreparedStatement;
import openr66.database.DbSession;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;

/**
 * Abstract database table implementation
 *
 * @author Frederic Bregier
 *
 */
public abstract class AbstractDbData {
    /**
     * UpdatedInfo status
     * @author Frederic Bregier
     *
     */
    public static enum UpdatedInfo {
        /**
         * Unknown run status
         */
        UNKNOWN,
        /**
         * Not updated run status
         */
        NOTUPDATED,
        /**
         * Interrupted status (stop or cancel)
         */
        INTERRUPTED,
        /**
         * Updated run status meaning ready to be submitted
         */
        TOSUBMIT,
        /**
         * In error run status
         */
        INERROR,
        /**
         * Running status
         */
        RUNNING,
        /**
         * All done run status
         */
        DONE;
    }
    /**
     * The DbSession to use
     */
    protected final DbSession dbSession;
    /**
     * Abstract constructor to set the DbSession to use
     * @param dbSession
     */
    public AbstractDbData(DbSession dbSession) {
        this.dbSession = dbSession;
    }
    /**
     * Test the existence of the current object
     * @return True if the object exists
     * @throws OpenR66DatabaseException
     */
    public abstract boolean exist() throws OpenR66DatabaseException;
    /**
     * Select object from table
     * @throws OpenR66DatabaseException
     */
    public abstract void select() throws OpenR66DatabaseException;
    /**
     * Insert object into table
     * @throws OpenR66DatabaseException
     */
    public abstract void insert() throws OpenR66DatabaseException;
    /**
     * Update object to table
     * @throws OpenR66DatabaseException
     */
    public abstract void update() throws OpenR66DatabaseException;
    /**
     * Delete object from table
     * @throws OpenR66DatabaseException
     */
    public abstract void delete() throws OpenR66DatabaseException;
    /**
     * Change UpdatedInfo status
     * @param info
     */
    public abstract void changeUpdatedInfo(UpdatedInfo info);
    /**
     * Internal function to set to Array used to push data to database
     */
    protected abstract void setToArray();
    /**
     * Internal function to retrieve data from Array to pull data from databasre
     * @throws OpenR66DatabaseSqlError
     */
    protected abstract void setFromArray() throws OpenR66DatabaseSqlError;
    /**
     * Set Value into PreparedStatement
     * @param ps
     * @param value
     * @param rank
     * @throws OpenR66DatabaseSqlError
     */
    static public void setTrueValue(PreparedStatement ps, DbValue value, int rank)
            throws OpenR66DatabaseSqlError {
        try {
            switch (value.type) {
                case Types.VARCHAR:
                    if (value.value == null) {
                        ps.setNull(rank, Types.VARCHAR);
                        break;
                    }
                    ps.setString(rank, (String) value.value);
                    break;
                case Types.LONGVARCHAR:
                    if (value.value == null) {
                        ps.setNull(rank, Types.LONGVARCHAR);
                        break;
                    }
                    ps.setString(rank, (String) value.value);
                    break;
                case Types.BIT:
                    if (value.value == null) {
                        ps.setNull(rank, Types.BIT);
                        break;
                    }
                    ps.setBoolean(rank, (Boolean) value.value);
                    break;
                case Types.TINYINT:
                    if (value.value == null) {
                        ps.setNull(rank, Types.TINYINT);
                        break;
                    }
                    ps.setByte(rank, (Byte) value.value);
                    break;
                case Types.SMALLINT:
                    if (value.value == null) {
                        ps.setNull(rank, Types.SMALLINT);
                        break;
                    }
                    ps.setShort(rank, (Short) value.value);
                    break;
                case Types.INTEGER:
                    if (value.value == null) {
                        ps.setNull(rank, Types.INTEGER);
                        break;
                    }
                    ps.setInt(rank, (Integer) value.value);
                    break;
                case Types.BIGINT:
                    if (value.value == null) {
                        ps.setNull(rank, Types.BIGINT);
                        break;
                    }
                    ps.setLong(rank, (Long) value.value);
                    break;
                case Types.REAL:
                    if (value.value == null) {
                        ps.setNull(rank, Types.REAL);
                        break;
                    }
                    ps.setFloat(rank, (Float) value.value);
                    break;
                case Types.DOUBLE:
                    if (value.value == null) {
                        ps.setNull(rank, Types.DOUBLE);
                        break;
                    }
                    ps.setDouble(rank, (Double) value.value);
                    break;
                case Types.VARBINARY:
                    if (value.value == null) {
                        ps.setNull(rank, Types.VARBINARY);
                        break;
                    }
                    ps.setBytes(rank, (byte[]) value.value);
                    break;
                case Types.DATE:
                    if (value.value == null) {
                        ps.setNull(rank, Types.DATE);
                        break;
                    }
                    ps.setDate(rank, (Date) value.value);
                    break;
                case Types.TIMESTAMP:
                    if (value.value == null) {
                        ps.setNull(rank, Types.TIMESTAMP);
                        break;
                    }
                    ps.setTimestamp(rank, (Timestamp) value.value);
                    break;
                default:
                    throw new OpenR66DatabaseSqlError("Type not supported: " +
                            value.type + " at " + rank);
            }
        } catch (ClassCastException e) {
            throw new OpenR66DatabaseSqlError("Setting values casting error: " +
                    value.type + " at " + rank, e);
        } catch (SQLException e) {
            DbSession.error(e);
            throw new OpenR66DatabaseSqlError("Setting values in error: " +
                    value.type + " at " + rank, e);
        }
    }
    /**
     * Set one value to a DbPreparedStatement
     * @param preparedStatement
     * @param value
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    protected void setValue(DbPreparedStatement preparedStatement, DbValue value)
            throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        PreparedStatement ps = preparedStatement.getPreparedStatement();
        setTrueValue(ps, value, 1);
    }
    /**
     * Set several values to a DbPreparedStatement
     * @param preparedStatement
     * @param values
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    protected void setValues(DbPreparedStatement preparedStatement,
            DbValue[] values) throws OpenR66DatabaseNoConnectionError,
            OpenR66DatabaseSqlError {
        PreparedStatement ps = preparedStatement.getPreparedStatement();
        for (int i = 0; i < values.length; i ++) {
            DbValue value = values[i];
            setTrueValue(ps, value, i + 1);
        }
    }
    /**
     * Get one value into DbValue from ResultSet
     * @param rs
     * @param value
     * @throws OpenR66DatabaseSqlError
     */
    static public void getTrueValue(ResultSet rs, DbValue value)
            throws OpenR66DatabaseSqlError {
        try {
            switch (value.type) {
                case Types.VARCHAR:
                    value.value = rs.getString(value.column);
                    break;
                case Types.LONGVARCHAR:
                    value.value = rs.getString(value.column);
                    break;
                case Types.BIT:
                    value.value = rs.getBoolean(value.column);
                    break;
                case Types.TINYINT:
                    value.value = rs.getByte(value.column);
                    break;
                case Types.SMALLINT:
                    value.value = rs.getShort(value.column);
                    break;
                case Types.INTEGER:
                    value.value = rs.getInt(value.column);
                    break;
                case Types.BIGINT:
                    value.value = rs.getLong(value.column);
                    break;
                case Types.REAL:
                    value.value = rs.getFloat(value.column);
                    break;
                case Types.DOUBLE:
                    value.value = rs.getDouble(value.column);
                    break;
                case Types.VARBINARY:
                    value.value = rs.getBytes(value.column);
                    break;
                case Types.DATE:
                    value.value = rs.getDate(value.column);
                    break;
                case Types.TIMESTAMP:
                    value.value = rs.getTimestamp(value.column);
                    break;
                default:
                    throw new OpenR66DatabaseSqlError("Type not supported: " +
                            value.type + " for " + value.column);
            }
        } catch (SQLException e) {
            DbSession.error(e);
            throw new OpenR66DatabaseSqlError("Getting values in error: " +
                    value.type + " for " + value.column, e);
        }
    }
    /**
     * Get one value into DbValue from DbPreparedStatement
     * @param preparedStatement
     * @param value
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    protected void getValue(DbPreparedStatement preparedStatement, DbValue value)
            throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        ResultSet rs = preparedStatement.getResultSet();
        getTrueValue(rs, value);
    }
    /**
     * Get several values into DbValue from DbPreparedStatement
     * @param preparedStatement
     * @param values
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    protected void getValues(DbPreparedStatement preparedStatement,
            DbValue[] values) throws OpenR66DatabaseNoConnectionError,
            OpenR66DatabaseSqlError {
        ResultSet rs = preparedStatement.getResultSet();
        for (DbValue value: values) {
            getTrueValue(rs, value);
        }
    }
}
