/**
 * This file is part of GoldenGate Project (named also GoldenGate or GG).
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All GoldenGate Project is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 * 
 * GoldenGate is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with GoldenGate . If not,
 * see <http://www.gnu.org/licenses/>.
 */
package openr66.protocol.localhandler;

import static openr66.context.R66FiniteDualStates.ERROR;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66Exception;
import openr66.protocol.exception.OpenR66ExceptionTrappedFactory;
import openr66.protocol.exception.OpenR66ProtocolBusinessNoWriteBackException;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolShutdownException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.packet.AbstractLocalPacket;
import openr66.protocol.localhandler.packet.ErrorPacket;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
import openr66.protocol.utils.ChannelUtils;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * @author frederic bregier
 */
public class LocalClientHandler extends SimpleChannelHandler {
	/**
	 * Internal Logger
	 */
	private static final GgInternalLogger logger = GgInternalLoggerFactory
			.getLogger(LocalClientHandler.class);

	/**
	 * Local Channel Reference
	 */
	private volatile LocalChannelReference localChannelReference = null;

	/*
	 * (non-Javadoc)
	 * @see org.jboss.netty.channel.SimpleChannelHandler#channelClosed(org.jboss.
	 * netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelStateEvent)
	 */
	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		logger.debug("Local Client Channel Closed: {}", e.getChannel().getId());
	}

	/*
	 * (non-Javadoc)
	 * @see org.jboss.netty.channel.SimpleChannelHandler#channelConnected(org.jboss
	 * .netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelStateEvent)
	 */
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		logger
				.debug("Local Client Channel Connected: " +
						e.getChannel().getId());
	}

	/**
	 * Initiate the LocalChannelReference
	 * 
	 * @param channel
	 * @throws InterruptedException
	 * @throws OpenR66ProtocolNetworkException
	 */
	private void initLocalClientHandler(Channel channel)
			throws InterruptedException, OpenR66ProtocolNetworkException {
		int i = 0;
		if (localChannelReference == null) {
			for (i = 0; i < Configuration.RETRYNB; i++) {
				localChannelReference = Configuration.configuration
						.getLocalTransaction().getFromId(channel.getId());
				if (localChannelReference != null) {
					return;
				}
				Thread.sleep(Configuration.RETRYINMS);
			}
			logger.warn("Cannot find local connection");
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.jboss.netty.channel.SimpleChannelHandler#messageReceived(org.jboss
	 * .netty.channel.ChannelHandlerContext, org.jboss.netty.channel.MessageEvent)
	 */
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		if (localChannelReference == null) {
			initLocalClientHandler(e.getChannel());
		}
		// only Startup Packet should arrived here !
		final AbstractLocalPacket packet = (AbstractLocalPacket) e.getMessage();
		if (packet.getType() != LocalPacketFactory.STARTUPPACKET) {
			logger.error("Local Client Channel Recv wrong packet: " +
					e.getChannel().getId() + " : " + packet.toString());
			throw new OpenR66ProtocolSystemException("Should not be here");
		}
		logger.debug("LocalClientHandler initialized: " +
				(localChannelReference != null));
	}

	/*
	 * (non-Javadoc)
	 * @see org.jboss.netty.channel.SimpleChannelHandler#exceptionCaught(org.jboss
	 * .netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ExceptionEvent)
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		// informs network of the problem
		logger.debug(
				"Local Client Channel Exception: {}", e.getChannel().getId(), e
						.getCause());
		if (localChannelReference == null) {
			initLocalClientHandler(e.getChannel());
		}
		if (localChannelReference != null) {
			OpenR66Exception exception = OpenR66ExceptionTrappedFactory
					.getExceptionFromTrappedException(e.getChannel(), e);
			localChannelReference.sessionNewState(ERROR);
			if (exception != null) {
				if (exception instanceof OpenR66ProtocolShutdownException) {
					Thread thread = new Thread(new ChannelUtils(), "R66 Shutdown Thread");
					thread.setDaemon(true);
					thread.start();
					logger.debug("Will close channel");
					Channels.close(e.getChannel());
					return;
				} else if (exception instanceof OpenR66ProtocolBusinessNoWriteBackException) {
					logger.error("Will close channel", exception);
					Channels.close(e.getChannel());
					return;
				} else if (exception instanceof OpenR66ProtocolNoConnectionException) {
					logger.error("Will close channel", exception);
					Channels.close(e.getChannel());
					return;
				}
				final ErrorPacket errorPacket = new ErrorPacket(exception
						.getMessage(),
						ErrorCode.RemoteError.getCode(), ErrorPacket.FORWARDCLOSECODE);
				ChannelUtils.writeAbstractLocalPacket(localChannelReference, errorPacket)
						.awaitUninterruptibly();
				if (!localChannelReference.getFutureRequest().isDone()) {
					localChannelReference.invalidateRequest(new R66Result(
							exception, localChannelReference.getSession(), true,
							ErrorCode.Internal, null));
				}
			} else {
				// Nothing to do
				return;
			}
		}
		logger.debug("Will close channel");
		ChannelUtils.close(e.getChannel());
	}

}
