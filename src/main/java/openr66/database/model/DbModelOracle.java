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

import java.sql.Types;

/**
 * Oracle Database Model implementation
 * @author Frederic Bregier
 *
 */
public class DbModelOracle extends AbstractDbModel {
    private static enum DBType {
        VARCHAR(Types.VARCHAR, " VARCHAR(255) "),
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
    }

    @Override
    public void createTables() {

    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.model.AbstractDbModel#nextSequence()
     */
    @Override
    public long nextSequence() {
        // TODO Auto-generated method stub
        return 0;
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.model.AbstractDbModel#resetSequence()
     */
    @Override
    public void resetSequence() {
        // TODO Auto-generated method stub

    }
}
