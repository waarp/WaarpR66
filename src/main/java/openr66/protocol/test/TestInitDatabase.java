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
package openr66.protocol.test;

import goldengate.common.logging.GgSlf4JLoggerFactory;

import java.io.File;

import openr66.database.DbConstant;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.database.model.DbModelFactory;
import openr66.protocol.config.AuthenticationFileBasedConfiguration;
import openr66.protocol.config.FileBasedConfiguration;
import openr66.protocol.config.RuleFileBasedConfiguration;
import openr66.protocol.exception.OpenR66ProtocolSystemException;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * @author Frederic Bregier
 *
 */
public class TestInitDatabase {

    /**
     * @param args
     */
    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        if (args.length < 4) {
            System.err
                    .println("Need databaseMode connectionString User Passwd");
            return;
        }
        /*
         * H2: "jdbc:h2:/data/test;AUTO_SERVER=TRUE" ;USER=sa;PASSWORD=123
         * ;IFEXISTS=TRUE ?? ;MODE=Oracle
         */
        String connection = // "jdbc:h2:D:/GG/R66/data/openr66;MODE=Oracle;AUTO_SERVER=TRUE";
        args[1];
        try {
            try {
                DbModelFactory.initialize(args[0], connection, args[2],
                        args[3], true);
            } catch (OpenR66DatabaseNoConnectionError e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }
            if (args.length == 4) {
                // Init database
                initdb();
                System.out.println("End");
            } else {
                // Init database
                initdb();
                System.out.println("End creation");
                // load Rules
                File dirConfig = new File(args[4]);
                if (dirConfig.isDirectory()) {
                    loadRules(dirConfig);
                } else {
                    System.err.println("Dir is not a directory: " + args[4]);
                }
                if (args.length > 5) {
                    loadHostAuth(args[5]);
                    if (args.length > 6) {
                        loadConfiguration(args[6]);
                    }
                }
                System.out.println("Load done");
            }
        } finally {
            try {
                if (DbConstant.admin != null) {
                    DbConstant.admin.close();
                }
            } catch (OpenR66DatabaseSqlError e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static void initdb() {
        // Create tables: configuration, hosts, rules, runner, cptrunner
        DbModelFactory.dbModel.createTables();
    }

    public static void loadRules(File dirConfig) {
        try {
            RuleFileBasedConfiguration.importRules(dirConfig);
        } catch (OpenR66ProtocolSystemException e3) {
            e3.printStackTrace();
        } catch (OpenR66DatabaseException e) {
            e.printStackTrace();
        }
    }
    public static void loadHostAuth(String filename) {
        AuthenticationFileBasedConfiguration.loadAuthentication(filename);
    }
    public static void loadConfiguration(String filename) {
        Document document = null;
        // Open config file
        try {
            document = new SAXReader().read(filename);
        } catch (DocumentException e) {
            e.printStackTrace();
            return;
        }
        FileBasedConfiguration.loadLimit(document);
    }
}
