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

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.FailedChannelFuture;
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
    public static final ChannelFutureListener channelClosedLogger =
        new ChannelFutureListener() {
        public void operationComplete(ChannelFuture future) {
            logger.warn("Channel closed", new OpenR66ProtocolSystemException());
        }
    };
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
     * @param message
     * @return ChannelFuture
     */
    public static ChannelFuture write(Channel channel, Object message) {
        if (channel.isConnected()) {
            return Channels.write(channel, message);
        }
        return new FailedChannelFuture(channel,new OpenR66ProtocolNetworkException("Not connected"));
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
     * Exit global ChannelFactory
     */
    public static void exit() {
        Configuration.configuration.isShutdown = true;
        final long delay = Configuration.TIMEOUTCON;
        logger.warn("Exit: Give a delay of " + delay + " ms");
        try {
            Thread.sleep(delay);
        } catch (final InterruptedException e) {
        }
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
