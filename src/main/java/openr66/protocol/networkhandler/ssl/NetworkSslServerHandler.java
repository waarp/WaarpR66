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
package openr66.protocol.networkhandler.ssl;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.handler.ssl.SslHandler;

import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.networkhandler.NetworkServerHandler;
import openr66.protocol.utils.R66Future;

/**
 * @author Frederic Bregier
 *
 */
public class NetworkSslServerHandler extends NetworkServerHandler {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(NetworkSslServerHandler.class);
    /**
     * Waiter for SSL handshake is finished
     */
    private static final ConcurrentHashMap<Integer, R66Future> waitForSsl
        = new ConcurrentHashMap<Integer, R66Future>();
    /**
     * Remover from SSL HashMap
     */
    private static final ChannelFutureListener remover = new ChannelFutureListener() {
        public void operationComplete(ChannelFuture future) {
            logger.debug("SSL remover");
            waitForSsl.remove(future.getChannel().getId());
        }
    };
    /**
     * Add the Channel as SSL handshake is over
     * @param channel
     */
    private static void addSslConnectedChannel(Channel channel) {
        R66Future futureSSL = new R66Future(true);
        waitForSsl.put(channel.getId(),futureSSL);
        channel.getCloseFuture().addListener(remover);
    }
    /**
     * Set the future of SSL handshake to status
     * @param channel
     * @param status
     */
    private static void setStatusSslConnectedChannel(Channel channel, boolean status) {
        R66Future futureSSL = waitForSsl.get(channel.getId());
        if (futureSSL != null) {
            if (status) {
                futureSSL.setSuccess();
            } else {
                futureSSL.cancel();
            }
        }
    }
    /**
     *
     * @param channel
     * @return True if the SSL handshake is over and OK, else False
     */
    public static boolean isSslConnectedChannel(Channel channel) {
        R66Future futureSSL = waitForSsl.get(channel.getId());
        if (futureSSL == null) {
            logger.error("No wait For SSL found");
            return false;
        } else {
            try {
                futureSSL.await(Configuration.configuration.TIMEOUTCON);
            } catch (InterruptedException e) {
            }
            if (futureSSL.isDone()) {
                logger.info("Wait For SSL: "+futureSSL.isSuccess());
                return futureSSL.isSuccess();
            }
            logger.error("Out of time for wait For SSL");
            return false;
        }
    }

    /* (non-Javadoc)
     * @see org.jboss.netty.channel.SimpleChannelHandler#channelOpen(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        Channel channel = e.getChannel();
        logger.debug("Add channel to ssl");
        addSslConnectedChannel(channel);
        isSSL = true;
        super.channelOpen(ctx, e);
    }
    /*
     * (non-Javadoc)
     *
     * @see
     * openr66.protocol.networkhandler.NetworkServerHandler#channelConnected
     * (org.jboss.netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws OpenR66ProtocolNetworkException {
        // Get the SslHandler in the current pipeline.
        // We added it in NetworkSslServerPipelineFactory.
        final SslHandler sslHandler = ctx.getPipeline().get(SslHandler.class);
        if (sslHandler != null) {
            // Get the SslHandler and begin handshake ASAP.
            // Get notified when SSL handshake is done.
            ChannelFuture handshakeFuture;
            handshakeFuture = sslHandler.handshake();
            handshakeFuture.addListener(new ChannelFutureListener() {
                public void operationComplete(ChannelFuture future)
                        throws Exception {
                    logger.info("Handshake: "+future.isSuccess(),future.getCause());
                    if (future.isSuccess()) {
                        setStatusSslConnectedChannel(future.getChannel(), true);
                    } else {
                        setStatusSslConnectedChannel(future.getChannel(), false);
                        future.getChannel().close();
                    }
                }
            });
        } else {
            logger.error("SSL Not found");
        }
        super.channelConnected(ctx, e);
    }
}
