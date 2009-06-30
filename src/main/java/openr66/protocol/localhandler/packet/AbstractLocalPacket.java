/**
 * 
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

    public abstract void createHeader() throws OpenR66ProtocolPacketException;

    public abstract void createMiddle() throws OpenR66ProtocolPacketException;

    public abstract void createEnd() throws OpenR66ProtocolPacketException;

    public abstract byte getType();

    @Override
    public abstract String toString();

    public ChannelBuffer getLocalPacket() throws OpenR66ProtocolPacketException {
        final ChannelBuffer buf = ChannelBuffers.buffer(4 * 3 + 1);// 3 header
        // lengths+type
        if (header == null) {
            createHeader();
        }
        final ChannelBuffer newHeader = (header != null) ? header
                : ChannelBuffers.EMPTY_BUFFER;
        final int headerLength = 4 * 2 + 1 + newHeader.readableBytes();
        if (middle == null) {
            createMiddle();
        }
        final ChannelBuffer newMiddle = (middle != null) ? middle
                : ChannelBuffers.EMPTY_BUFFER;
        final int middleLength = newMiddle.readableBytes();
        if (end == null) {
            createEnd();
        }
        final ChannelBuffer newEnd = (end != null) ? end
                : ChannelBuffers.EMPTY_BUFFER;
        final int endLength = newEnd.readableBytes();
        buf.writeInt(headerLength);
        buf.writeInt(middleLength);
        buf.writeInt(endLength);
        buf.writeByte(getType());
        final ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer(buf,
                newHeader, newMiddle, newEnd);
        return channelBuffer;
    }
}
