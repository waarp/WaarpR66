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
package openr66.protocol.utils;

import goldengate.common.file.DataBlock;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import openr66.database.data.DbTaskRunner;
import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.DataPacket;
import openr66.protocol.localhandler.packet.EndTransferPacket;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.networkhandler.packet.NetworkPacket;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.ChannelGroupFutureListener;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

/**
 * @author Frederic Bregier
 */
public class ChannelUtils implements Runnable {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(ChannelUtils.class);

    public static final Integer NOCHANNEL = Integer.MIN_VALUE;

    /**
     * Get the Remote InetAddress
     *
     * @param channel
     * @return the remote InetAddress
     */
    public static InetAddress getRemoteInetAddress(Channel channel) {
        InetSocketAddress socketAddress = (InetSocketAddress) channel
                .getRemoteAddress();
        if (socketAddress == null) {
            socketAddress = new InetSocketAddress(20);
        }
        return socketAddress.getAddress();
    }

    /**
     * Get the Local InetAddress
     *
     * @param channel
     * @return the local InetAddress
     */
    public static InetAddress getLocalInetAddress(Channel channel) {
        final InetSocketAddress socketAddress = (InetSocketAddress) channel
                .getLocalAddress();
        return socketAddress.getAddress();
    }

    /**
     * Get the Remote InetSocketAddress
     *
     * @param channel
     * @return the remote InetSocketAddress
     */
    public static InetSocketAddress getRemoteInetSocketAddress(Channel channel) {
        return (InetSocketAddress) channel.getRemoteAddress();
    }

    /**
     * Get the Local InetSocketAddress
     *
     * @param channel
     * @return the local InetSocketAddress
     */
    public static InetSocketAddress getLocalInetSocketAddress(Channel channel) {
        return (InetSocketAddress) channel.getLocalAddress();
    }

    /**
     * Finalize resources attached to handlers
     *
     * @author Frederic Bregier
     */
    private static class R66ChannelGroupFutureListener implements
            ChannelGroupFutureListener {
        OrderedMemoryAwareThreadPoolExecutor pool;

        ChannelFactory channelFactory;

        public R66ChannelGroupFutureListener(
                OrderedMemoryAwareThreadPoolExecutor pool,
                ChannelFactory channelFactory) {
            this.pool = pool;
            this.channelFactory = channelFactory;
        }

        public void operationComplete(ChannelGroupFuture future)
                throws Exception {
            pool.shutdownNow();
            channelFactory.releaseExternalResources();
        }
    }

    /**
     * Terminate all registered channels
     *
     * @return the number of previously registered network channels
     */
    private static int terminateCommandChannels() {
        final int result = Configuration.configuration.getServerChannelGroup()
                .size();
        logger.info("ServerChannelGroup: " + result);
        Configuration.configuration.getServerChannelGroup().close()
                .addListener(
                        new R66ChannelGroupFutureListener(
                                Configuration.configuration
                                        .getServerPipelineExecutor(),
                                Configuration.configuration
                                        .getServerChannelFactory()));
        return result;
    }

    /**
     * Return the current number of network connections
     *
     * @param configuration
     * @return the current number of network connections
     */
    public static int nbCommandChannels(Configuration configuration) {
        return configuration.getServerChannelGroup().size();
    }

    /**
     *
     * @param channel
     */
    public static void close(Channel channel) {
        try {
            Thread.sleep(Configuration.WAITFORNETOP);
        } catch (InterruptedException e) {
        }
        Channels.close(channel);
    }

    /**
     *
     * @param localChannelReference
     * @param runner
     * @param networkChannel
     * @param block
     * @return
     * @throws OpenR66ProtocolPacketException
     */
    public static ChannelFuture writeBackDataBlock(
            LocalChannelReference localChannelReference, DbTaskRunner runner,
            Channel networkChannel, DataBlock block)
            throws OpenR66ProtocolPacketException {
        // FIXME if MD5
        ChannelBuffer md5 = ChannelBuffers.EMPTY_BUFFER;
        if (runner.getMode() == RequestPacket.RECVMD5MODE ||
                runner.getMode() == RequestPacket.SENDMD5MODE) {
            md5 = FileUtils.getHash(block.getBlock());
        }
        DataPacket data = new DataPacket(runner.getRank(), block.getBlock()
                .copy(), md5);
        NetworkPacket networkPacket;
        try {
            networkPacket = new NetworkPacket(localChannelReference
                    .getLocalId(), localChannelReference.getRemoteId(), data);
        } catch (OpenR66ProtocolPacketException e) {
            logger.error("Cannot construct message from " + data.toString(), e);
            throw e;
        }
        ChannelFuture future = Channels.write(networkChannel, networkPacket);
        runner.incrementRank();
        return future;
    }
    /**
    *
    * @param localChannelReference
    * @param runner
    * @param networkChannel
    * @param block
    * @return
    * @throws OpenR66ProtocolPacketException
    */
   public static ChannelFuture writeValidEndTransfer(
           LocalChannelReference localChannelReference, DbTaskRunner runner,
           Channel networkChannel)
           throws OpenR66ProtocolPacketException {
       EndTransferPacket packet = new EndTransferPacket(LocalPacketFactory.REQUESTPACKET);
       NetworkPacket networkPacket;
       try {
           networkPacket = new NetworkPacket(localChannelReference
                   .getLocalId(), localChannelReference.getRemoteId(), packet);
       } catch (OpenR66ProtocolPacketException e) {
           logger.error("Cannot construct message from " + packet.toString(), e);
           throw e;
       }
       ChannelFuture future = Channels.write(networkChannel, networkPacket);
       return future;
   }
    /**
     * Exit global ChannelFactory
     */
    public static void exit() {
        Configuration.configuration.isShutdown = true;
        final long delay = Configuration.configuration.TIMEOUTCON;
        // Inform others that shutdown
        Configuration.configuration.getLocalTransaction()
                .shutdownLocalChannels();
        logger.warn("Exit: Give a delay of " + delay + " ms");
        try {
            Thread.sleep(delay);
        } catch (final InterruptedException e) {
        }
        NetworkTransaction.closeRetrieveExecutors();
        Configuration.configuration.getGlobalTrafficShapingHandler()
                .releaseExternalResources();
        logger.warn("Exit Shutdown Command");
        terminateCommandChannels();
        logger.warn("Exit Shutdown Local");
        Configuration.configuration.getLocalTransaction().closeAll();
        logger.warn("Exit end of Shutdown");
    }

    /**
     * This function is the top function to be called when the server is to be
     * shutdown.
     */
    @Override
    public void run() {
        OpenR66SignalHandler.terminate(true);
    }
}
