/**
 * 
 */
package openr66.protocol.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * This class represents Abstract Packet with its header, middle and end parts.
 * 
 * 
 * A Packet is composed of one Header part, one Middle part (data), and one End part.
 * 
 * Header: length field (4 bytes) = Middle length field (4 bytes), End length field (4 bytes), type field (1 byte), ...<br>
 * Middle: (Middle length field bytes)<br>
 * End: (End length field bytes) = code status field (4 bytes), ...<br>
 * 
 * @author frederic bregier
 * 
 */
public abstract class AbstractPacket {
    protected ChannelBuffer header;

    protected ChannelBuffer middle;

    protected ChannelBuffer end;

    public abstract void createHeader() throws OpenR66ProtocolPacketException;

    public abstract void createMiddle() throws OpenR66ProtocolPacketException;

    public abstract void createEnd() throws OpenR66ProtocolPacketException;

    public abstract void updateHeader() throws OpenR66ProtocolPacketException;

    public abstract void updateMiddle() throws OpenR66ProtocolPacketException;

    public abstract void updateEnd() throws OpenR66ProtocolPacketException;

    public ChannelBuffer getPacket() {
        ChannelBuffer buf = ChannelBuffers.buffer(Integer.SIZE*3);// 3 header lengths
        int headerLength = header.readableBytes()+Integer.SIZE*2;
        int middleLength = middle.readableBytes();
        int endLength = end.readableBytes();
        buf.writeInt(headerLength);
        buf.writeInt(middleLength);
        buf.writeInt(endLength);
        ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer(buf, header,
                middle, end);
        return channelBuffer;
    }
}
