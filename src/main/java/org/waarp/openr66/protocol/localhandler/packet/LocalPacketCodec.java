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
package org.waarp.openr66.protocol.localhandler.packet;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOutbandHandler;
import io.netty.channel.ChannelEvent;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.Channels;
import io.netty.channel.MessageEvent;
import io.netty.handler.codec.frame.FrameDecoder;
import org.waarp.common.exception.InvalidArgumentException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;

/**
 * Local Packet Decoder
 * 
 * @author Frederic Bregier
 */
public class LocalPacketCodec extends FrameDecoder implements
		ChannelOutbandHandler {

	/*
	 * (non-Javadoc)
	 * @see io.netty.handler.codec.frame.FrameDecoder#decode(io.netty
	 * .channel.ChannelHandlerContext, io.netty.channel.Channel,
	 * io.netty.buffer.ByteBuf)
	 */
	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ByteBuf buf) throws Exception {
		// Make sure if the length field was received.
		if (buf.readableBytes() < 4) {
			// The length field was not received yet - return null.
			// This method will be invoked again when more packets are
			// received and appended to the buffer.
			return null;
		}
		return decodeNetworkPacket(buf);
	}

	public static AbstractLocalPacket decodeNetworkPacket(ByteBuf buf)
			throws OpenR66ProtocolPacketException {
		// Mark the current buffer position
		buf.markReaderIndex();
		// Read the length field
		final int length = buf.readInt();
		if (buf.readableBytes() < length) {
			buf.resetReaderIndex();
			return null;
		}
		// Now we can read the header
		// Header: Header length field (4 bytes) = Middle length field (4
		// bytes), End length field (4 bytes), type field (1 byte), ...
		final int middleLength = buf.readInt();
		final int endLength = buf.readInt();
		// check if the packet is complete
		if (middleLength + endLength + length - 8 > buf.readableBytes()) {
			buf.resetReaderIndex();
			return null;
		}
		// createPacketFromByteBuf read the buffer
		return LocalPacketFactory.createPacketFromByteBuf(length - 8,
				middleLength, endLength, buf);
	}

	public void handleOutband(ChannelHandlerContext ctx, ChannelEvent e)
			throws Exception {
		if (e instanceof MessageEvent) {
			final MessageEvent evt = (MessageEvent) e;
			if (evt.getMessage() instanceof ByteBuf) {
				Channels.writeAndFlush(ctx, evt.getFuture(), evt.getMessage());
				return;
			}
			if (!(evt.getMessage() instanceof AbstractLocalPacket)) {
				throw new InvalidArgumentException("Incorrect write object: " +
						evt.getMessage().getClass().getName());
			}
			final AbstractLocalPacket packet = (AbstractLocalPacket) evt
					.getMessage();
			final ByteBuf buf = packet.getLocalPacket(null);
			Channels.writeAndFlush(ctx, evt.getFuture(), buf);
		} else {
			ctx.sendOutband(e);
		}
	}

}
