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

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import openr66.database.R66DbPreparedStatement;
import openr66.database.R66DbSession;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;

/**
 * @author Frederic Bregier
 *
 */
public abstract class AbstractDbData {

    public abstract void select() throws OpenR66DatabaseException;

    public abstract void insert() throws OpenR66DatabaseException;

    public abstract void update() throws OpenR66DatabaseException;

    public abstract void delete() throws OpenR66DatabaseException;

    private void setTrueValue(PreparedStatement ps, DbValue value, int rank)
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
                    throw new OpenR66DatabaseSqlError("Type not supported: "+value.type+" at "+rank);
            }
        } catch (SQLException e) {
            R66DbSession.error(e);
            throw new OpenR66DatabaseSqlError("Setting values in error: "+value.type+" at "+rank, e);
        }
    }
    protected void setValue(R66DbPreparedStatement preparedStatement,
            DbValue value) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        PreparedStatement ps = preparedStatement.getPreparedStatement();
        this.setTrueValue(ps, value, 1);
    }
    protected void setValues(R66DbPreparedStatement preparedStatement,
            DbValue[] values) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        PreparedStatement ps = preparedStatement.getPreparedStatement();
        for (int i = 0; i < values.length; i++) {
            DbValue value = values[i];
            this.setTrueValue(ps, value, i+1);
        }
    }

    protected void getTrueValue(ResultSet rs,
            DbValue value) throws OpenR66DatabaseSqlError {
        try {
            switch (value.type) {
                case Types.VARCHAR:
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
                    throw new OpenR66DatabaseSqlError("Type not supported: "+value.type+" for "+value.column);
            }
        } catch (SQLException e) {
            R66DbSession.error(e);
            throw new OpenR66DatabaseSqlError("Getting values in error: "+value.type+" for "+value.column, e);
        }
    }
    protected void getValue(R66DbPreparedStatement preparedStatement,
            DbValue value) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        ResultSet rs = preparedStatement.getResultSet();
        this.getTrueValue(rs, value);
    }
    protected void getValues(R66DbPreparedStatement preparedStatement,
            DbValue[] values) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        ResultSet rs = preparedStatement.getResultSet();
        for (int i = 0; i < values.length; i++) {
            DbValue value = values[i];
            this.getTrueValue(rs, value);
        }
    }
}
