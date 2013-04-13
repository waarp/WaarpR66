/**
 * This file is part of Waarp Project.
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All Waarp Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * 
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with Waarp . If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.protocol.networkhandler.ssl;


import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.handler.ssl.SslHandler;
import org.waarp.common.crypto.ssl.WaarpSslUtility;
import org.waarp.common.future.WaarpFuture;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNetworkException;
import org.waarp.openr66.protocol.networkhandler.NetworkServerHandler;

/**
 * @author Frederic Bregier
 * 
 */
public class NetworkSslServerHandler extends NetworkServerHandler {
	/**
	 * @param isServer
	 */
	public NetworkSslServerHandler(boolean isServer) {
		super(isServer);
	}

	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(NetworkSslServerHandler.class);

	/**
	 * 
	 * @param channel
	 * @return True if the SSL handshake is over and OK, else False
	 */
	public static boolean isSslConnectedChannel(Channel channel) {
		
		WaarpFuture futureSSL = WaarpSslUtility.getFutureSslHandshake(channel);
		if (futureSSL == null) {
			for (int i = 0; i < Configuration.RETRYNB; i++) {
				futureSSL = WaarpSslUtility.getFutureSslHandshake(channel);
				if (futureSSL != null)
					break;
				try {
					Thread.sleep(Configuration.RETRYINMS);
				} catch (InterruptedException e) {
				}
			}
		}
		if (futureSSL == null) {
			logger.debug("No wait For SSL found");
			return false;
		} else {
			try {
				futureSSL.await(Configuration.configuration.TIMEOUTCON);
			} catch (InterruptedException e) {
			}
			if (futureSSL.isDone()) {
				logger.debug("Wait For SSL: " + futureSSL.isSuccess());
				return futureSSL.isSuccess();
			}
			logger.error("Out of time for wait For SSL");
			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.jboss.netty.channel.SimpleChannelHandler#channelOpen(org.jboss.netty.channel.
	 * ChannelHandlerContext, org.jboss.netty.channel.ChannelStateEvent)
	 */
	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		Channel channel = e.getChannel();
		logger.debug("Add channel to ssl");
		WaarpSslUtility.addSslOpenedChannel(channel);
		isSSL = true;
		super.channelOpen(ctx, e);
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.networkhandler.NetworkServerHandler#channelConnected
	 * (org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelStateEvent)
	 */
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws OpenR66ProtocolNetworkException {
		// Get the SslHandler in the current pipeline.
		// We added it in NetworkSslServerPipelineFactory.
		final ChannelHandler handler = ctx.getPipeline().getFirst();
		if (handler instanceof SslHandler) {
			final SslHandler sslHandler = (SslHandler) handler;
			if (sslHandler.isIssueHandshake()) {
				// client side
				WaarpSslUtility.setStatusSslConnectedChannel(ctx.getChannel(), true);
			} else {
				// server side
				// Get the SslHandler and begin handshake ASAP.
				// Get notified when SSL handshake is done.
				if (! WaarpSslUtility.runHandshake(ctx.getChannel())) {
					Configuration.configuration.r66Mib.notifyError(
							"SSL Connection Error", "During Handshake");
				}
			}				
		} else {
			logger.error("SSL Not found");
		}
		super.channelConnected(ctx, e);
	}
}
