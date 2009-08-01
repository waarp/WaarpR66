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
package openr66.database.data;

import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;

import openr66.database.exception.OpenR66DatabaseSqlError;

/**
 * Database Value to help getting and setting value from and to database
 * @author Frederic Bregier
 *
 */
public class DbValue {
    /**
     * Real value
     */
    public Object value;
    /**
     * Data Type
     */
    public int type;
    /**
     * Column name
     */
    public String column;

    public DbValue(String value) {
        this.value = value;
        type = Types.VARCHAR;
    }

    public DbValue(String value, boolean LONG) {
        this.value = value;
        type = Types.LONGVARCHAR;
    }

    public DbValue(boolean value) {
        this.value = value;
        type = Types.BIT;
    }

    public DbValue(byte value) {
        this.value = value;
        type = Types.TINYINT;
    }

    public DbValue(short value) {
        this.value = value;
        type = Types.SMALLINT;
    }

    public DbValue(int value) {
        this.value = value;
        type = Types.INTEGER;
    }

    public DbValue(long value) {
        this.value = value;
        type = Types.BIGINT;
    }

    public DbValue(float value) {
        this.value = value;
        type = Types.REAL;
    }

    public DbValue(double value) {
        this.value = value;
        type = Types.DOUBLE;
    }

    public DbValue(byte[] value) {
        this.value = value;
        type = Types.VARBINARY;
    }

    public DbValue(Date value) {
        this.value = value;
        type = Types.DATE;
    }

    public DbValue(Timestamp value) {
        this.value = value;
        type = Types.TIMESTAMP;
    }

    public DbValue(java.util.Date value) {
        this.value = new Timestamp(value.getTime());
        type = Types.TIMESTAMP;
    }

    public DbValue(String value, String name) {
        this.value = value;
        type = Types.VARCHAR;
        column = name;
    }

    public DbValue(String value, String name, boolean LONG) {
        this.value = value;
        type = Types.LONGVARCHAR;
        column = name;
    }

    public DbValue(boolean value, String name) {
        this.value = value;
        type = Types.BIT;
        column = name;
    }

    public DbValue(byte value, String name) {
        this.value = value;
        type = Types.TINYINT;
        column = name;
    }

    public DbValue(short value, String name) {
        this.value = value;
        type = Types.SMALLINT;
        column = name;
    }

    public DbValue(int value, String name) {
        this.value = value;
        type = Types.INTEGER;
        column = name;
    }

    public DbValue(long value, String name) {
        this.value = value;
        type = Types.BIGINT;
        column = name;
    }

    public DbValue(float value, String name) {
        this.value = value;
        type = Types.REAL;
        column = name;
    }

    public DbValue(double value, String name) {
        this.value = value;
        type = Types.DOUBLE;
        column = name;
    }

    public DbValue(byte[] value, String name) {
        this.value = value;
        type = Types.VARBINARY;
        column = name;
    }

    public DbValue(Date value, String name) {
        this.value = value;
        type = Types.DATE;
        column = name;
    }

    public DbValue(Timestamp value, String name) {
        this.value = value;
        type = Types.TIMESTAMP;
        column = name;
    }

    public DbValue(java.util.Date value, String name) {
        this.value = new Timestamp(value.getTime());
        type = Types.TIMESTAMP;
        column = name;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setValue(boolean value) {
        this.value = value;
    }

    public void setValue(byte value) {
        this.value = value;
    }

    public void setValue(short value) {
        this.value = value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public void setValue(Date value) {
        this.value = value;
    }

    public void setValue(Timestamp value) {
        this.value = value;
    }

    public void setValue(java.util.Date value) {
        this.value = new Timestamp(value.getTime());
    }

    public Object getValue() throws OpenR66DatabaseSqlError {
        switch (type) {
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.VARBINARY:
            case Types.DATE:
            case Types.TIMESTAMP:
                return value;
            default:
                throw new OpenR66DatabaseSqlError("Type unknown: " + type);
        }
    }

    public String getValueAsString() throws OpenR66DatabaseSqlError {
        switch (type) {
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return (String) value;
            case Types.BIT:
                return ((Boolean) value).toString();
            case Types.TINYINT:
                return ((Byte) value).toString();
            case Types.SMALLINT:
                return ((Short) value).toString();
            case Types.INTEGER:
                return ((Integer) value).toString();
            case Types.BIGINT:
                return ((Long) value).toString();
            case Types.REAL:
                return ((Float) value).toString();
            case Types.DOUBLE:
                return ((Double) value).toString();
            case Types.VARBINARY:
                return new String((byte[]) value);
            case Types.DATE:
                return ((Date) value).toString();
            case Types.TIMESTAMP:
                return ((Timestamp) value).toString();
            default:
                throw new OpenR66DatabaseSqlError("Type unknown: " + type);
        }
    }
}
