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
package openr66.protocol.test;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import openr66.context.R66Result;
import openr66.protocol.config.Configuration;
import openr66.protocol.config.R66FileBasedConfiguration;
import openr66.protocol.exception.OpenR66Exception;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
import openr66.protocol.localhandler.packet.ShutdownPacket;
import openr66.protocol.localhandler.packet.ValidPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.ChannelUtils;

import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * @author Frederic Bregier
 */
public class TestClientShutdown {

    /**
     * @param args
     * @throws OpenR66ProtocolPacketException
     */
    public static void main(String[] args)
            throws OpenR66ProtocolPacketException {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        final GgInternalLogger logger = GgInternalLoggerFactory
                .getLogger(TestClientShutdown.class);
        if (args.length < 1) {
            logger
                    .error("Needs at least the configuration file as first argument");
            return;
        }
        Configuration.configuration.fileBasedConfiguration = new R66FileBasedConfiguration();
        if (!Configuration.configuration.fileBasedConfiguration
                .setConfigurationFromXml(args[0])) {
            logger
                    .error("Needs a correct configuration file as first argument");
            return;
        }
        Configuration.configuration.pipelineInit();

        final ShutdownPacket packet = new ShutdownPacket(
                Configuration.configuration.getSERVERADMINKEY());
        final NetworkTransaction networkTransaction = new NetworkTransaction();
        final SocketAddress socketServerAddress = new InetSocketAddress(
                Configuration.configuration.SERVER_PORT);
        LocalChannelReference localChannelReference = null;
        OpenR66Exception lastException = null;
        for (int i = 0; i < Configuration.RETRYNB; i ++) {
            try {
                localChannelReference = networkTransaction
                        .createConnection(socketServerAddress);
                break;
            } catch (OpenR66ProtocolNetworkException e1) {
                lastException = e1;
                localChannelReference = null;
            } catch (OpenR66ProtocolRemoteShutdownException e1) {
                lastException = e1;
                localChannelReference = null;
                break;
            } catch (OpenR66ProtocolNoConnectionException e1) {
                lastException = e1;
                localChannelReference = null;
                break;
            }
        }
        if (localChannelReference == null) {
            logger.error("Cannot connect", lastException);
            networkTransaction.closeAll();
            return;
        } else if (lastException != null) {
            logger.warn("Connection retry since ", lastException);
        }
        logger.warn("Start");
        ChannelUtils.writeAbstractLocalPacket(localChannelReference, packet);
        localChannelReference.getFutureRequest().awaitUninterruptibly();
        if (localChannelReference.getFutureRequest().isSuccess()) {
            logger.warn("Shutdown OK");
        } else {
            R66Result result = localChannelReference.getFutureRequest()
                    .getResult();
            if (result.other instanceof ValidPacket &&
                    ((ValidPacket) result.other).getTypeValid() == LocalPacketFactory.SHUTDOWNPACKET) {
                logger.warn("Shutdown command OK");
            } else {
                logger.warn("Cannot Shutdown", localChannelReference
                        .getFutureRequest().getCause());
            }
        }
        networkTransaction.closeAll();
    }

}
