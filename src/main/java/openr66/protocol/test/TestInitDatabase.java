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
package openr66.protocol.test;

import openr66.database.DbConstant;
import openr66.database.R66DbAdmin;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.database.model.DbModelFactory;

/**
 * @author Frederic Bregier
 *
 */
public class TestInitDatabase {

    /**
     * @param args
     */
    public static void main(String[] args) {
        /*
         * H2: "jdbc:h2:/data/test;AUTO_SERVER=TRUE"
        ;USER=sa;PASSWORD=123
        ;IFEXISTS=TRUE ??
        ;MODE=Oracle
         */
        String connection = "jdbc:h2:D:/GG/R66/data/config;MODE=Oracle;AUTO_SERVER=TRUE";
        try {
            try {
                DbConstant.admin = new R66DbAdmin("h2",
                        connection, "openr66", "openr66", true);
            } catch (OpenR66DatabaseNoConnectionError e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }
            if (args.length == 0) {
                // Init database
                initdb();
            } else {
                // Do something with database
            }
        } finally {
            try {
                DbConstant.admin.close();
            } catch (OpenR66DatabaseSqlError e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static void initdb() {
        // Create tables: configuration, hosts, rules, runner, cptrunner
        DbModelFactory.initialize();
        DbModelFactory.dbModel.createTables();
    }
}
