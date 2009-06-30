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

import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.localhandler.packet.ShutdownPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.networkhandler.packet.NetworkPacket;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
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
        final NetworkTransaction networkTransaction = new NetworkTransaction();
        final SocketAddress socketServerAddress = new InetSocketAddress(
                Configuration.SERVER_PORT);
        final ShutdownPacket packet = new ShutdownPacket("password");
        final NetworkPacket networkPacket = new NetworkPacket(-1, 0, packet
                .getLocalPacket());
        logger.warn("START");
        final Channel channel = networkTransaction
                .createNewClient(socketServerAddress);
        Channels.write(channel, networkPacket);
        try {
            Thread.sleep(10000);
        } catch (final InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        networkTransaction.closeAll();
    }

}
