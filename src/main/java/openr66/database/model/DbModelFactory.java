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

import openr66.database.DbAdmin;
import openr66.database.DbConstant;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;

/**
 * Factory to store the Database Model object
 *
 * @author Frederic Bregier
 *
 */
public class DbModelFactory {

    /**
     * Info on JDBC Class is already loaded or not
     */
    static public volatile boolean classLoaded = false;
    /**
     * Database Model Object
     */
    public static DbModel dbModel;
    /**
     * Initialize the Database Model according to arguments.
     * @param dbdriver
     * @param dbserver
     * @param dbuser
     * @param dbpasswd
     * @param write
     * @throws OpenR66DatabaseNoConnectionError
     */
    public static void initialize(String dbdriver, String dbserver,
            String dbuser, String dbpasswd, boolean write)
            throws OpenR66DatabaseNoConnectionError {
        DbType type = DbType.getFromDriver(dbdriver);
        switch (type) {
            case H2:
                dbModel = new DbModelH2();
                break;
            case Oracle:
                dbModel = new DbModelOracle();
                break;
            case PostGreSQL:
                dbModel = new DbModelPostgresql();
                break;
            case MySQL:
                dbModel = new DbModelMysql();
                break;
            default:
                throw new OpenR66DatabaseNoConnectionError(
                        "TypeDriver unknown: " + type);
        }
        DbConstant.admin = new DbAdmin(type, dbserver, dbuser, dbpasswd,
                write);
    }
}
