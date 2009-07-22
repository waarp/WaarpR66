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
package openr66.database.model;

import java.sql.Types;

import openr66.database.DbConstant;
import openr66.database.DbRequest;
import openr66.database.data.DbR66HostAuth;
import openr66.database.data.DbR66Configuration;
import openr66.database.data.DbR66Rule;
import openr66.database.data.DbTaskRunner;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;

/**
 * @author Frederic Bregier
 *
 */
public class DbModelH2 extends AbstractDbModel {
    private static enum DBType {
        VARCHAR(Types.VARCHAR, " VARCHAR(254) "),
        BIT(Types.BIT, " BOOLEAN "),
        TINYINT(Types.TINYINT, " TINYINT "),
        SMALLINT(Types.SMALLINT, " SMALLINT "),
        INTEGER(Types.INTEGER, " INTEGER "),
        BIGINT(Types.BIGINT," BIGINT "),
        REAL(Types.REAL," REAL "),
        DOUBLE(Types.DOUBLE, " DOUBLE "),
        VARBINARY(Types.VARBINARY, " BINARY "),
        DATE(Types.DATE, " DATE "),
        TIMESTAMP(Types.TIMESTAMP, " TIMESTAMP ");

        public int type;
        public String name;
        public String constructor;

        private DBType(int type, String constructor) {
            this.type = type;
            this.name = name();
            this.constructor = constructor;
        }

        public static String getType(int sqltype) {
            switch (sqltype) {
                case Types.VARCHAR:
                    return VARCHAR.constructor;
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
            }
            return null;
        }
    }
    @Override
    public void createTables() {
        // Create tables: configuration, hosts, rules, runner, cptrunner
        String createTableH2 =
            "CREATE TABLE IF NOT EXISTS ";
        String primaryKey = " PRIMARY KEY ";
        String notNull = " NOT NULL ";

        // Configuration
        String action = createTableH2+DbR66Configuration.table+"(";
        DbR66Configuration.Columns []ccolumns = DbR66Configuration.Columns.values();
        for (int i = 0; i < ccolumns.length-1; i++) {
            action += ccolumns[i].name()+
                DBType.getType(DbR66Configuration.dbTypes[i])+
                notNull+", ";
        }
        action += ccolumns[ccolumns.length-1].name()+
            DBType.getType(DbR66Configuration.dbTypes[ccolumns.length-1])+
            primaryKey+")";
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
        action = createTableH2+DbR66HostAuth.table+"(";
        DbR66HostAuth.Columns []hcolumns = DbR66HostAuth.Columns.values();
        for (int i = 0; i < hcolumns.length-1; i++) {
            action += hcolumns[i].name()+
                DBType.getType(DbR66HostAuth.dbTypes[i])+
                notNull+", ";
        }
        action += hcolumns[hcolumns.length-1].name()+
            DBType.getType(DbR66HostAuth.dbTypes[hcolumns.length-1])+
            primaryKey+")";
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
        action = createTableH2+DbR66Rule.table+"(";
        DbR66Rule.Columns []rcolumns = DbR66Rule.Columns.values();
        for (int i = 0; i < rcolumns.length-1; i++) {
            action += rcolumns[i].name()+
                DBType.getType(DbR66Rule.dbTypes[i])+
                ", ";
        }
        action += rcolumns[rcolumns.length-1].name()+
            DBType.getType(DbR66Rule.dbTypes[rcolumns.length-1])+
            primaryKey+")";
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
        action = createTableH2+DbTaskRunner.table+"(";
        DbTaskRunner.Columns []acolumns = DbTaskRunner.Columns.values();
        for (int i = 0; i < acolumns.length-1; i++) {
            action += acolumns[i].name()+
                DBType.getType(DbTaskRunner.dbTypes[i])+
                notNull+", ";
        }
        action += acolumns[acolumns.length-1].name()+
            DBType.getType(DbTaskRunner.dbTypes[acolumns.length-1])+
            primaryKey+")";
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
        action = "CREATE SEQUENCE IF NOT EXISTS "+DbTaskRunner.fieldseq+
            " START WITH "+(DbConstant.ILLEGALVALUE+1);
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
    /* (non-Javadoc)
     * @see openr66.database.model.AbstractDbModel#resetSequence()
     */
    @Override
    public void resetSequence() {
        String action = "ALTER SEQUENCE "+DbTaskRunner.fieldseq+
            " RESTART WITH "+(DbConstant.ILLEGALVALUE+1);
        System.out.println(action);
    }
    /* (non-Javadoc)
     * @see openr66.database.model.AbstractDbModel#nextSequence()
     */
    @Override
    public long nextSequence() {
        String action = "SELECT NEXTVAL('"+DbTaskRunner.fieldseq+"')";
        System.out.println(action);
        return 0;
    }
}
