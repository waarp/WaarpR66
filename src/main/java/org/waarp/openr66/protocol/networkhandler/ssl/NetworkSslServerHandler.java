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
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;

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
			int maxtry = (int) (Configuration.configuration.TIMEOUTCON / Configuration.RETRYINMS) / 2;
			for (int i = 0; i < maxtry; i++) {
				futureSSL = WaarpSslUtility.getFutureSslHandshake(channel);
				if (futureSSL != null)
					break;
				try {
					Thread.sleep(Configuration.RETRYINMS);
					Thread.yield();
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

	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		Channel channel = e.getChannel();
		logger.debug("Add channel to ssl");
		WaarpSslUtility.addSslOpenedChannel(channel);
		isSSL = true;
		super.channelOpen(ctx, e);
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws OpenR66ProtocolNetworkException {
		// Check first if allowed
		Channel networkChannel = e.getChannel();
		if (NetworkTransaction.isBlacklisted(networkChannel)) {
			logger.warn("Connection refused since Partner is in BlackListed from "+networkChannel.getRemoteAddress().toString());
			isBlackListed = true;
			Configuration.configuration.r66Mib.notifyError(
					"Black Listed connection temptative", "During Handshake");
			// close immediately the connection
			WaarpSslUtility.closingSslChannel(networkChannel);
			return;
		}
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
