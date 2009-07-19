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
import java.util.ArrayList;

import openr66.database.R66DbPreparedStatement;
import openr66.database.R66DbSession;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;

/**
 * @author Frederic Bregier
 *
 */
public abstract class AbstractDbData {

    protected abstract void select();

    protected abstract void insert();

    protected abstract void update();

    protected abstract void delete();

    protected abstract String getXML();

    protected void setValues(R66DbPreparedStatement preparedStatement,
            ArrayList<DbValue> values) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        PreparedStatement ps = preparedStatement.getPreparedStatement();
        for (int i = 1; i <= values.size(); i++) {
            DbValue value = values.get(i);
            try {
                switch (value.type) {
                    case Types.VARCHAR:
                        if (value.value == null) {
                            ps.setNull(i, Types.VARCHAR);
                            break;
                        }
                        ps.setString(i, (String) value.value);
                        break;
                    case Types.BIT:
                        if (value.value == null) {
                            ps.setNull(i, Types.BIT);
                            break;
                        }
                        ps.setBoolean(i, (Boolean) value.value);
                        break;
                    case Types.TINYINT:
                        if (value.value == null) {
                            ps.setNull(i, Types.TINYINT);
                            break;
                        }
                        ps.setByte(i, (Byte) value.value);
                        break;
                    case Types.SMALLINT:
                        if (value.value == null) {
                            ps.setNull(i, Types.SMALLINT);
                            break;
                        }
                        ps.setShort(i, (Short) value.value);
                        break;
                    case Types.INTEGER:
                        if (value.value == null) {
                            ps.setNull(i, Types.INTEGER);
                            break;
                        }
                        ps.setInt(i, (Integer) value.value);
                        break;
                    case Types.BIGINT:
                        if (value.value == null) {
                            ps.setNull(i, Types.BIGINT);
                            break;
                        }
                        ps.setLong(i, (Long) value.value);
                        break;
                    case Types.REAL:
                        if (value.value == null) {
                            ps.setNull(i, Types.REAL);
                            break;
                        }
                        ps.setFloat(i, (Float) value.value);
                        break;
                    case Types.DOUBLE:
                        if (value.value == null) {
                            ps.setNull(i, Types.DOUBLE);
                            break;
                        }
                        ps.setDouble(i, (Double) value.value);
                        break;
                    case Types.VARBINARY:
                        if (value.value == null) {
                            ps.setNull(i, Types.VARBINARY);
                            break;
                        }
                        ps.setBytes(i, (byte[]) value.value);
                        break;
                    case Types.DATE:
                        if (value.value == null) {
                            ps.setNull(i, Types.DATE);
                            break;
                        }
                        ps.setDate(i, (Date) value.value);
                        break;
                    case Types.TIMESTAMP:
                        if (value.value == null) {
                            ps.setNull(i, Types.TIMESTAMP);
                            break;
                        }
                        ps.setTimestamp(i, (Timestamp) value.value);
                        break;
                    default:
                        throw new OpenR66DatabaseSqlError("Type not supported: "+value.type+" at "+i);
                }
            } catch (SQLException e) {
                R66DbSession.error(e);
                throw new OpenR66DatabaseSqlError("Setting values in error: "+value.type+" at "+i, e);
            }
        }
    }

    protected void getValues(R66DbPreparedStatement preparedStatement,
            ArrayList<DbValue> values) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        ResultSet rs = preparedStatement.getResultSet();
        for (int i = 1; i <= values.size(); i++) {
            DbValue value = values.get(i);
            try {
                switch (value.type) {
                    case Types.VARCHAR:
                        value.value = rs.getString(i);
                        break;
                    case Types.BIT:
                        value.value = rs.getBoolean(i);
                        break;
                    case Types.TINYINT:
                        value.value = rs.getByte(i);
                        break;
                    case Types.SMALLINT:
                        value.value = rs.getShort(i);
                        break;
                    case Types.INTEGER:
                        value.value = rs.getInt(i);
                        break;
                    case Types.BIGINT:
                        value.value = rs.getLong(i);
                        break;
                    case Types.REAL:
                        value.value = rs.getFloat(i);
                        break;
                    case Types.DOUBLE:
                        value.value = rs.getDouble(i);
                        break;
                    case Types.VARBINARY:
                        value.value = rs.getBytes(i);
                        break;
                    case Types.DATE:
                        value.value = rs.getDate(i);
                        break;
                    case Types.TIMESTAMP:
                        value.value = rs.getTimestamp(i);
                        break;
                    default:
                        throw new OpenR66DatabaseSqlError("Type not supported: "+value.type+" at "+i);
                }
            } catch (SQLException e) {
                R66DbSession.error(e);
                throw new OpenR66DatabaseSqlError("Getting values in error: "+value.type+" at "+i, e);
            }
        }
    }
}
