/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors. This is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of the License,
 * or (at your option) any later version. This software is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * software; if not, write to the Free Software Foundation, Inc., 51 Franklin
 * St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF site:
 * http://www.fsf.org.
 */
package openr66.server;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;
import openr66.configuration.FileBasedConfiguration;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.utils.OpenR66SignalHandler;

import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

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
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        final GgInternalLogger logger = GgInternalLoggerFactory
                .getLogger(R66Server.class);
        if (args.length < 1) {
            logger
                    .error("Needs at least the configuration file as first argument");
            return;
        }
        if (! FileBasedConfiguration
                .setConfigurationFromXml(args[0])) {
            logger
                    .error("Needs a correct configuration file as first argument");
            return;
        }
        try {
            Configuration.configuration.serverStartup();
        } catch (OpenR66DatabaseException e) {
            logger
                .error("Startup of server is in error");
            OpenR66SignalHandler.terminate(true);
        }
        logger.warn("Server OpenR66 starts for "+Configuration.configuration.HOST_ID);
    }

}
