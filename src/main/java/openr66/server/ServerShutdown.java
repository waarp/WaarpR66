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

import goldengate.common.digest.MD5;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;

import java.net.SocketAddress;

import openr66.configuration.FileBasedConfiguration;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.database.DbConstant;
import openr66.database.data.DbHostAuth;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
import openr66.protocol.localhandler.packet.ShutdownPacket;
import openr66.protocol.localhandler.packet.ValidPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.ChannelUtils;

import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * Local client to shutdown the server (using network)
 *
 * @author Frederic Bregier
 */
public class ServerShutdown {

    /**
     * @param args the configuration file as first argument
     * @throws OpenR66ProtocolPacketException
     */
    public static void main(String[] args)
            throws OpenR66ProtocolPacketException {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        final GgInternalLogger logger = GgInternalLoggerFactory
                .getLogger(ServerShutdown.class);
        if (args.length < 1) {
            logger
                    .error("Needs the configuration file as first argument");
            ChannelUtils.stopLogger();
            System.exit(1);
            return;
        }
        if (! FileBasedConfiguration
                .setConfigurationServerShutdownFromXml(args[0])) {
            logger
                    .error("Needs a correct configuration file as first argument");
            if (DbConstant.admin != null){
                DbConstant.admin.close();
            }
            ChannelUtils.stopLogger();
            System.exit(1);
            return;
        }
        Configuration.configuration.pipelineInit();
        byte []key = MD5.passwdCrypt(Configuration.configuration.getSERVERADMINKEY());
        final ShutdownPacket packet = new ShutdownPacket(
                key);
        final NetworkTransaction networkTransaction = new NetworkTransaction();
        DbHostAuth host = Configuration.configuration.HOST_SSLAUTH;
        final SocketAddress socketServerAddress = host.getSocketAddress();
        LocalChannelReference localChannelReference = null;
        localChannelReference = networkTransaction
            .createConnectionWithRetry(socketServerAddress,true, null);
        if (localChannelReference == null) {
            logger.error("Cannot connect");
            networkTransaction.closeAll();
            return;
        }
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
            } else if (result.code == ErrorCode.Shutdown) {
                logger.warn("Shutdown command On going");
            } else {
                logger.error("Cannot Shutdown: "+result.toString(), localChannelReference
                        .getFutureRequest().getCause());
            }
        }
        networkTransaction.closeAll();
    }

}
