/**
   This file is part of GoldenGate Project (named also GoldenGate or GG).

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All GoldenGate Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   GoldenGate is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GoldenGate .  If not, see <http://www.gnu.org/licenses/>.
 */
package openr66.server;

import goldengate.common.database.exception.GoldenGateDatabaseNoConnectionError;
import goldengate.common.database.exception.GoldenGateDatabaseSqlError;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;

import java.io.File;

import openr66.configuration.AuthenticationFileBasedConfiguration;
import openr66.configuration.FileBasedConfiguration;
import openr66.configuration.RuleFileBasedConfiguration;
import openr66.database.DbConstant;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolBusinessException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.utils.ChannelUtils;

import org.jboss.netty.logging.InternalLoggerFactory;

/**
 * Server local configuration export to files
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
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(null));
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
            String directory = args[1];
            String hostname = Configuration.configuration.HOST_ID;
            logger.warn("Start of Export");
            File dir = new File(directory);
            if (! dir.isDirectory()) {
                dir.mkdirs();
            }
            try {
                RuleFileBasedConfiguration.writeXml(directory, hostname);
            } catch (GoldenGateDatabaseNoConnectionError e1) {
                logger.error("Error",e1);
                DbConstant.admin.close();
                ChannelUtils.stopLogger();
                System.exit(2);
            } catch (GoldenGateDatabaseSqlError e1) {
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
            } catch (GoldenGateDatabaseNoConnectionError e1) {
                logger.error("Error",e1);
                DbConstant.admin.close();
                ChannelUtils.stopLogger();
                System.exit(2);
            } catch (GoldenGateDatabaseSqlError e1) {
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
            } catch (GoldenGateDatabaseNoConnectionError e) {
                logger.error("Error",e);
                DbConstant.admin.close();
                ChannelUtils.stopLogger();
                System.exit(2);
            } catch (GoldenGateDatabaseSqlError e) {
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
