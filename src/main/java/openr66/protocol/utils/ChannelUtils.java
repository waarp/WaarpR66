/**
 * Copyright 2009, Frederic Bregier, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package openr66.protocol.utils;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import openr66.protocol.config.Configuration;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.ChannelGroupFutureListener;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

/**
 * @author Frederic Bregier
 *
 */
public class ChannelUtils {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(ChannelUtils.class);
    
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
        InetSocketAddress socketAddress = (InetSocketAddress) channel
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
     *
     */
    private static class R66ChannelGroupFutureListener implements
            ChannelGroupFutureListener {
        OrderedMemoryAwareThreadPoolExecutor pool;

        ChannelFactory channelFactory;

        ChannelFactory channelFactory2;

        public R66ChannelGroupFutureListener(
                OrderedMemoryAwareThreadPoolExecutor pool,
                ChannelFactory channelFactory, ChannelFactory channelFactory2) {
            this.pool = pool;
            this.channelFactory = channelFactory;
            this.channelFactory2 = channelFactory2;
        }

        public void operationComplete(ChannelGroupFuture future)
                throws Exception {
            pool.shutdownNow();
            channelFactory.releaseExternalResources();
            if (channelFactory2 != null) {
                channelFactory2.releaseExternalResources();
            }
        }
    }

    /**
     * Terminate all registered channels
     *
     * @param configuration
     * @return the number of previously registered network channels
     */
    private static int terminateCommandChannels(Configuration configuration) {
        int result = configuration.getServerChannelGroup().size();
        configuration.getServerChannelGroup()
                .close().addListener(
                        new R66ChannelGroupFutureListener(configuration
                                .getServerPipelineExecutor(), configuration
                                .getServerChannelFactory(), null));
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
     * Exit global ChannelFactory
     *
     * @param configuration
     */
    public static void exit(Configuration configuration) {
        configuration.isShutdown = true;
        long delay = 2 * Configuration.TIMEOUTCON;
        logger.warn("Exit: Give a delay of " + delay + " ms");
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
        }
        configuration.getGlobalTrafficShapingHandler().releaseExternalResources();
        logger.warn("Exit Shutdown Command");
        terminateCommandChannels(configuration);
        logger.warn("Exit end of Shutdown");
    }

    /**
     * This function is the top function to be called when the server is to be
     * shutdown.
     *
     * @param configuration
     */
    public static void teminateServer(Configuration configuration) {
        OpenR66SignalHandler.terminate(true, configuration);
    }
}
