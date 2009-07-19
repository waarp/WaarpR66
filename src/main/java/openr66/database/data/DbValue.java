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

import java.sql.Timestamp;
import java.sql.Types;
import java.sql.Date;

import openr66.database.exception.OpenR66DatabaseSqlError;

/**
 * @author Frederic Bregier
 *
 */
public class DbValue {
    public Object value;
    public int type;


    public DbValue(String value) {
        this.value = value;
        this.type = Types.VARCHAR;
    }
    public DbValue(boolean value) {
        this.value = (Boolean) value;
        this.type = Types.BIT;
    }
    public DbValue(byte value) {
        this.value = (Byte) value;
        this.type = Types.TINYINT;
    }
    public DbValue(short value) {
        this.value = (Short) value;
        this.type = Types.SMALLINT;
    }
    public DbValue(int value) {
        this.value = (Integer) value;
        this.type = Types.INTEGER;
    }
    public DbValue(long value) {
        this.value = (Long) value;
        this.type = Types.BIGINT;
    }
    public DbValue(float value) {
        this.value = (Float) value;
        this.type = Types.REAL;
    }
    public DbValue(double value) {
        this.value = (Double) value;
        this.type = Types.DOUBLE;
    }
    public DbValue(byte[] value) {
        this.value = value;
        this.type = Types.VARBINARY;
    }
    public DbValue(Date value) {
        this.value = value;
        this.type = Types.DATE;
    }
    public DbValue(Timestamp value) {
        this.value = value;
        this.type = Types.TIMESTAMP;
    }
    public DbValue(java.util.Date value) {
        this.value = new Timestamp(value.getTime());
        this.type = Types.TIMESTAMP;
    }
    public Object getValue() throws OpenR66DatabaseSqlError {
        switch (this.type) {
            case Types.VARCHAR:
            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.VARBINARY:
            case Types.DATE:
                return this.value;
            case Types.TIMESTAMP:
                return new Date(((Timestamp) value).getTime());
            default:
                throw new OpenR66DatabaseSqlError("Type unknown: "+this.type);
        }
    }
}
