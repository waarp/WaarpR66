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
package openr66.server;

import java.io.File;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;

import openr66.configuration.AuthenticationFileBasedConfiguration;
import openr66.configuration.RuleFileBasedConfiguration;
import openr66.database.DbConstant;
import openr66.database.data.DbTaskRunner;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.database.model.DbModelFactory;
import openr66.protocol.exception.OpenR66ProtocolSystemException;

import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * Server configuration export to files
 *
 * @author Frederic Bregier
 *
 */
public class ServerExportConfiguration {
    /**
     * Internal Logger
     */
    private static GgInternalLogger logger;

    /**
     *
     * @param args as databaseMode connectionString User Passwd directory_to_export Hostname
     */
    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(ServerExportConfiguration.class);
        }
        if (args.length < 6) {
            System.err
                    .println("Need databaseMode connectionString User Passwd and the directory where to export and Hostname");
            return;
        }
        try {
            try {
                DbModelFactory.initialize(args[0], args[1], args[2],
                        args[3], true);
            } catch (OpenR66DatabaseNoConnectionError e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }
            String directory = args[4];
            String hostname = args[5];
            logger.warn("Start");
            File dir = new File(directory);
            if (! dir.isDirectory()) {
                dir.mkdirs();
            }
            try {
                RuleFileBasedConfiguration.writeXml(directory, hostname);
            } catch (OpenR66DatabaseNoConnectionError e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            } catch (OpenR66DatabaseSqlError e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            } catch (OpenR66ProtocolSystemException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            String filename = dir.getAbsolutePath()+File.separator+hostname+"_Runners.run.xml";
            try {
                DbTaskRunner.writeXML(filename);
            } catch (OpenR66DatabaseNoConnectionError e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            } catch (OpenR66DatabaseSqlError e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            filename = dir.getAbsolutePath()+File.separator+hostname+"_Authentications.xml";
            try {
                AuthenticationFileBasedConfiguration.writeXML(filename);
            } catch (OpenR66DatabaseNoConnectionError e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (OpenR66DatabaseSqlError e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (OpenR66ProtocolSystemException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            logger.error("End");
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

}
