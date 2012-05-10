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

import goldengate.common.database.exception.GoldenGateDatabaseException;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;
import openr66.configuration.FileBasedConfiguration;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.utils.OpenR66SignalHandler;

import org.jboss.netty.logging.InternalLoggerFactory;

/**
 * R66Server startup main class
 *
 * @author Frederic Bregier
 */
public class R66Server {

    /**
     * @param args as first argument the configuration file
     * @throws OpenR66ProtocolPacketException
     */
    public static void main(String[] args)
            throws OpenR66ProtocolPacketException {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(null));
        final GgInternalLogger logger = GgInternalLoggerFactory
                .getLogger(R66Server.class);
        if (args.length < 1) {
            logger
                    .error("Needs the configuration file as first argument");
            return;
        }
        if (! FileBasedConfiguration
                .setConfigurationServerFromXml(Configuration.configuration, args[0])) {
            logger
                    .error("Needs a correct configuration file as first argument");
            return;
        }
        try {
            Configuration.configuration.serverStartup();
        } catch (GoldenGateDatabaseException e) {
            logger
                .error("Startup of server is in error");
            OpenR66SignalHandler.terminate(false);
        }
        logger.warn("Server OpenR66 starts for "+Configuration.configuration.HOST_ID);
        System.err.println("Server OpenR66 starts for "+Configuration.configuration.HOST_ID);
    }

}
