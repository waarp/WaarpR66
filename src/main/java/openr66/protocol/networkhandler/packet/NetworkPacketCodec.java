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
package openr66.protocol.networkhandler.packet;

import goldengate.common.exception.InvalidArgumentException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

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
		final ChannelBuffer buffer = ChannelBuffers.buffer(length - 9);
		buf.readBytes(buffer, length - 9);
		return new NetworkPacket(localId, remoteId, code, buffer);
	}

	@Override
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
