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
import openr66.configuration.FileBasedConfiguration;
import openr66.configuration.RuleFileBasedConfiguration;
import openr66.database.DbConstant;
import openr66.database.data.DbTaskRunner;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolBusinessException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.utils.ChannelUtils;

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
     * @param args as configuration file and the directory where to export
     */
    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(ServerExportConfiguration.class);
        }
        if (args.length < 2) {
            System.err
                    .println("Need configuration file and the directory where to export");
            System.exit(1);
        }
        try {
            if (! FileBasedConfiguration
                    .setConfigurationServerMinimalFromXml(args[0])) {
                logger
                        .error("Needs a correct configuration file as first argument");
                if (DbConstant.admin != null){
                    DbConstant.admin.close();
                }
                ChannelUtils.stopLogger();
                System.exit(1);
                return;
            }
            /*Document document = null;
            String filename = args[0];
            // Open config file
            try {
                document = new SAXReader().read(filename);
            } catch (DocumentException e) {
                logger.error("Unable to read the XML Config file: " + filename, e);
                ChannelUtils.stopLogger();
                System.exit(1);
            }
            if (document == null) {
                logger.error("Unable to read the XML Config file: " + filename);
                ChannelUtils.stopLogger();
                System.exit(1);
            }
            if (! FileBasedConfiguration.loadCommon(document)) {
                logger.error("Unable to find Host ID in Config file: " + filename);
                ChannelUtils.stopLogger();
                System.exit(1);
            }
            if (! FileBasedConfiguration.loadDatabase(document)) {
                logger
                .error("Needs a correct configuration file as first argument");
                ChannelUtils.stopLogger();
                System.exit(1);
            }*/
            String directory = args[1];
            String hostname = Configuration.configuration.HOST_ID;
            logger.warn("Start of Export");
            File dir = new File(directory);
            if (! dir.isDirectory()) {
                dir.mkdirs();
            }
            try {
                RuleFileBasedConfiguration.writeXml(directory, hostname);
            } catch (OpenR66DatabaseNoConnectionError e1) {
                logger.error("Error",e1);
                DbConstant.admin.close();
                ChannelUtils.stopLogger();
                System.exit(2);
            } catch (OpenR66DatabaseSqlError e1) {
                logger.error("Error",e1);
                DbConstant.admin.close();
                ChannelUtils.stopLogger();
                System.exit(2);
            } catch (OpenR66ProtocolSystemException e1) {
                logger.error("Error",e1);
                DbConstant.admin.close();
                ChannelUtils.stopLogger();
                System.exit(2);
            }
            String filename = dir.getAbsolutePath()+File.separator+hostname+"_Runners.run.xml";
            try {
                DbTaskRunner.writeXMLWriter(filename);
            } catch (OpenR66DatabaseNoConnectionError e1) {
                logger.error("Error",e1);
                DbConstant.admin.close();
                ChannelUtils.stopLogger();
                System.exit(2);
            } catch (OpenR66DatabaseSqlError e1) {
                logger.error("Error",e1);
                DbConstant.admin.close();
                ChannelUtils.stopLogger();
                System.exit(2);
            } catch (OpenR66ProtocolBusinessException e1) {
                logger.error("Error",e1);
                DbConstant.admin.close();
                ChannelUtils.stopLogger();
                System.exit(2);
            }
            filename = dir.getAbsolutePath()+File.separator+hostname+"_Authentications.xml";
            try {
                AuthenticationFileBasedConfiguration.writeXML(filename);
            } catch (OpenR66DatabaseNoConnectionError e) {
                logger.error("Error",e);
                DbConstant.admin.close();
                ChannelUtils.stopLogger();
                System.exit(2);
            } catch (OpenR66DatabaseSqlError e) {
                logger.error("Error",e);
                DbConstant.admin.close();
                ChannelUtils.stopLogger();
                System.exit(2);
            } catch (OpenR66ProtocolSystemException e) {
                logger.error("Error",e);
                DbConstant.admin.close();
                ChannelUtils.stopLogger();
                System.exit(2);
            }
            logger.warn("End of Export");
        } finally {
            if (DbConstant.admin != null) {
                DbConstant.admin.close();
            }
        }
    }

}
