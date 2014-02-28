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
package org.waarp.openr66.protocol.networkhandler.packet;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.waarp.common.exception.InvalidArgumentException;
import org.waarp.openr66.protocol.localhandler.packet.KeepAlivePacket;
import org.waarp.openr66.protocol.localhandler.packet.LocalPacketCodec;
import org.waarp.openr66.protocol.localhandler.packet.LocalPacketFactory;
import org.waarp.openr66.protocol.localhandler.packet.NoOpPacket;
import org.waarp.openr66.protocol.networkhandler.NetworkChannelReference;
import org.waarp.openr66.protocol.networkhandler.NetworkServerHandler;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelUtils;

/**
 * Packet Decoder
 * 
 * @author Frederic Bregier
 */
public class NetworkPacketCodec extends FrameDecoder implements
		ChannelDownstreamHandler {
	/*
	 * (non-Javadoc)
	 * @see org.jboss.netty.handler.codec.frame.FrameDecoder#decode(org.jboss.netty
	 * .channel.ChannelHandlerContext, org.jboss.netty.channel.Channel,
	 * org.jboss.netty.buffer.ChannelBuffer)
	 */
	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buf) throws Exception {
		// Make sure if the length field was received.
		if (buf.readableBytes() < 4) {
			// The length field was not received yet - return null.
			// This method will be invoked again when more packets are
			// received and appended to the buffer.
			return null;
		}
		// Mark the current buffer position
		buf.markReaderIndex();
		// Read the length field
		final int length = buf.readInt();
		if (buf.readableBytes() < length) {
			buf.resetReaderIndex();
			return null;
		}
		// Now we can read the two Ids
		final int localId = buf.readInt();
		final int remoteId = buf.readInt();
		final byte code = buf.readByte();
		int readerInder = buf.readerIndex();
		ChannelBuffer buffer = buf.slice(readerInder, length - 9);
		buf.skipBytes(length - 9);
		NetworkPacket networkPacket = new NetworkPacket(localId, remoteId, code, buffer);
		if (code == LocalPacketFactory.KEEPALIVEPACKET) {
			KeepAlivePacket keepAlivePacket = (KeepAlivePacket)
					LocalPacketCodec.decodeNetworkPacket(networkPacket.getBuffer());
			if (keepAlivePacket.isToValidate()) {
				keepAlivePacket.validate();
				NetworkPacket response =
						new NetworkPacket(ChannelUtils.NOCHANNEL,
								ChannelUtils.NOCHANNEL, keepAlivePacket, null);
				NetworkChannelReference nc = NetworkTransaction.getImmediateNetworkChannel(channel);
				if (nc != null) {
					nc.useIfUsed();
				}
				Channels.write(channel, response);
			}
			// Replaced by a NoOp packet
			networkPacket = new NetworkPacket(localId, remoteId, new NoOpPacket(), null);
			NetworkServerHandler nsh = (NetworkServerHandler) ctx.getPipeline().getLast();
			nsh.setKeepAlivedSent();
		}
		return networkPacket;
	}

	public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e)
			throws Exception {
		if (e instanceof MessageEvent) {
			final MessageEvent evt = (MessageEvent) e;
			Object msg = evt.getMessage();
			if (!(msg instanceof NetworkPacket)) {
				throw new InvalidArgumentException("Incorrect write object: " +
						msg.getClass().getName());
			}
			final NetworkPacket packet = (NetworkPacket) msg;
			final ChannelBuffer finalBuf = packet.getNetworkPacket();
			Channels.write(ctx, evt.getFuture(), finalBuf);
		} else {
			ctx.sendDownstream(e);
		}
	}

}
