/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package openr66.protocol.packet;

import goldengate.common.exception.InvalidArgumentException;

import org.jboss.netty.buffer.ChannelBuffer;
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
 * 
 */
public class LocalPacketCodec extends FrameDecoder implements
        ChannelDownstreamHandler {

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jboss.netty.handler.codec.frame.FrameDecoder#decode(org.jboss.netty
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
        int length = buf.readInt();
        if (buf.readableBytes() < length) {
            buf.resetReaderIndex();
            return null;
        }
        // Now we can read the header
        // Header: Header length field (4 bytes) = Middle length field (4
        // bytes), End length field (4 bytes), type field (1 byte), ...
        int middleLength = buf.readInt();
        int endLength = buf.readInt();
        // check if the packet is complete
        if (middleLength + endLength + length - 8 > buf.readableBytes()) {
            buf.resetReaderIndex();
            return null;
        }
        // createPacketFromChannelBuffer read the buffer
        return LocalPacketFactory.createPacketFromChannelBuffer(length - 8,
                middleLength, endLength, buf);
    }

    @Override
    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e)
            throws Exception {
        if (e instanceof MessageEvent) {
            MessageEvent evt = (MessageEvent) e;
            if (!(evt.getMessage() instanceof AbstractLocalPacket)) {
                throw new InvalidArgumentException("Incorrect write object: " +
                        evt.getMessage().getClass().getName());
            }
            AbstractLocalPacket packet = (AbstractLocalPacket) evt.getMessage();
            ChannelBuffer buf = packet.getLocalPacket();
            Channels.write(ctx, evt.getFuture(), buf);
        } else {
            ctx.sendDownstream(e);
        }
    }

}
