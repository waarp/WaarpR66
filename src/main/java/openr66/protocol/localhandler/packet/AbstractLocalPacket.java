/**
   This file is part of GoldenGate Project (named also GoldenGate or GG).

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All GoldenGate Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   GoldenGate is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GoldenGate .  If not, see <http://www.gnu.org/licenses/>.
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * This class represents Abstract Packet with its header, middle and end parts.
 * A Packet is composed of one Header part, one Middle part (data), and one End
 * part. Header: length field (4 bytes) = Middle length field (4 bytes), End
 * length field (4 bytes), type field (1 byte), ...<br>
 * Middle: (Middle length field bytes)<br>
 * End: (End length field bytes) = code status field (4 bytes), ...<br>
 *
 * @author frederic bregier
 */
public abstract class AbstractLocalPacket {
    protected ChannelBuffer header;

    protected ChannelBuffer middle;

    protected ChannelBuffer end;

    public AbstractLocalPacket(ChannelBuffer header, ChannelBuffer middle,
            ChannelBuffer end) {
        this.header = header;
        this.middle = middle;
        this.end = end;
    }

    public AbstractLocalPacket() {
        header = null;
        middle = null;
        end = null;
    }
    /**
     * Prepare the Header buffer
     * @throws OpenR66ProtocolPacketException
     */
    public abstract void createHeader() throws OpenR66ProtocolPacketException;
    /**
     * Prepare the Middle buffer
     * @throws OpenR66ProtocolPacketException
     */
    public abstract void createMiddle() throws OpenR66ProtocolPacketException;
    /**
     * Prepare the End buffer
     * @throws OpenR66ProtocolPacketException
     */
    public abstract void createEnd() throws OpenR66ProtocolPacketException;
    /**
     *
     * @return the type of Packet
     */
    public abstract byte getType();

    @Override
    public abstract String toString();
    /**
     *
     * @return the ChannelBuffer as LocalPacket
     * @throws OpenR66ProtocolPacketException
     */
    public ChannelBuffer getLocalPacket() throws OpenR66ProtocolPacketException {
        final ChannelBuffer buf = ChannelBuffers.buffer(4 * 3 + 1);// 3 header
        // lengths+type
        if (header == null) {
            createHeader();
        }
        final ChannelBuffer newHeader = header != null? header
                : ChannelBuffers.EMPTY_BUFFER;
        final int headerLength = 4 * 2 + 1 + newHeader.readableBytes();
        if (middle == null) {
            createMiddle();
        }
        final ChannelBuffer newMiddle = middle != null? middle
                : ChannelBuffers.EMPTY_BUFFER;
        final int middleLength = newMiddle.readableBytes();
        if (end == null) {
            createEnd();
        }
        final ChannelBuffer newEnd = end != null? end
                : ChannelBuffers.EMPTY_BUFFER;
        final int endLength = newEnd.readableBytes();
        buf.writeInt(headerLength);
        buf.writeInt(middleLength);
        buf.writeInt(endLength);
        buf.writeByte(getType());
        final ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer(
                buf, newHeader, newMiddle, newEnd);
        return channelBuffer;
    }
}
