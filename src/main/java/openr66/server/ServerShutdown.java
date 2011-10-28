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

import goldengate.common.digest.FilesystemBasedDigest;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;

import java.net.SocketAddress;

import openr66.configuration.FileBasedConfiguration;
import openr66.context.ErrorCode;
import openr66.context.R66FiniteDualStates;
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
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(null));
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
        byte[] key;
        key = FilesystemBasedDigest.passwdCrypt(Configuration.configuration.getSERVERADMINKEY());
        final ShutdownPacket packet = new ShutdownPacket(
                key);
        final NetworkTransaction networkTransaction = new NetworkTransaction();
        DbHostAuth host = Configuration.configuration.HOST_SSLAUTH;
        final SocketAddress socketServerAddress = host.getSocketAddress();
        LocalChannelReference localChannelReference = null;
        localChannelReference = networkTransaction
            .createConnectionWithRetry(socketServerAddress,true, null);
        if (localChannelReference == null) {
            logger.error("Cannot connect to "+host.getSocketAddress());
            networkTransaction.closeAll();
            return;
        }
        localChannelReference.sessionNewState(R66FiniteDualStates.SHUTDOWN);
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
