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
 * A Packet is composed of one Header part, one Middle part (data), and one End
 * part.
 * 
 * Header: length field (4 bytes) = Middle length field (4 bytes), End length
 * field (4 bytes), type field (1 byte), ...<br>
 * Middle: (Middle length field bytes)<br>
 * End: (End length field bytes) = code status field (4 bytes), ...<br>
 * 
 * @author frederic bregier
 * 
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
        this.header = null;
        this.middle = null;
        this.end = null;
    }

    public abstract void createHeader() throws OpenR66ProtocolPacketException;

    public abstract void createMiddle() throws OpenR66ProtocolPacketException;

    public abstract void createEnd() throws OpenR66ProtocolPacketException;

    public abstract String toString();

    public ChannelBuffer getLocalPacket() throws OpenR66ProtocolPacketException {
        ChannelBuffer buf = ChannelBuffers.buffer(4 * 3);// 3 header lengths
        if (header == null) {
            this.createHeader();
        }
        ChannelBuffer newHeader = (header != null)? header
                : ChannelBuffers.EMPTY_BUFFER;
        int headerLength = 4 * 2 + newHeader.readableBytes();
        if (middle == null) {
            this.createMiddle();
        }
        ChannelBuffer newMiddle = (middle != null)? middle
                : ChannelBuffers.EMPTY_BUFFER;
        int middleLength = newMiddle.readableBytes();
        if (end == null) {
            this.createEnd();
        }
        ChannelBuffer newEnd = (end != null)? end : ChannelBuffers.EMPTY_BUFFER;
        int endLength = newEnd.readableBytes();
        buf.writeInt(headerLength);
        buf.writeInt(middleLength);
        buf.writeInt(endLength);
        ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer(buf,
                newHeader, newMiddle, newEnd);
        return channelBuffer;
    }
}
