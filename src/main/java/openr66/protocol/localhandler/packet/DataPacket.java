/**
 * 
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Send packet
 * 
 * header = packetRank
 * middle = data
 * 
 * @author frederic bregier
 */
public class DataPacket extends AbstractLocalPacket {
    private int packetRank;
    private int lengthPacket;
    private ChannelBuffer data;
    private ChannelBuffer key;
    
    public static DataPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf) throws OpenR66ProtocolPacketException {
        if (headerLength-1 <=0) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        if (middleLength <=0) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        int packetRank = buf.readInt();
        ChannelBuffer data = buf.readBytes(middleLength);
        ChannelBuffer key = (endLength > 0 ) ? buf.readBytes(endLength) :
            ChannelBuffers.EMPTY_BUFFER;
        return new DataPacket(packetRank, data, key);
    }
    
    public DataPacket(int packetRank, ChannelBuffer data, ChannelBuffer key) {
        this.packetRank = packetRank;
        this.data = data;
        this.key = (key == null) ? ChannelBuffers.EMPTY_BUFFER : key;
        this.lengthPacket = data.readableBytes();
    }
    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
     */
    @Override
    public void createEnd() throws OpenR66ProtocolPacketException {
        this.end = this.key;
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
     */
    @Override
    public void createHeader() throws OpenR66ProtocolPacketException {
        this.header = 
            ChannelBuffers.wrappedBuffer(Integer.toString(this.packetRank).getBytes());
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
     */
    @Override
    public void createMiddle() throws OpenR66ProtocolPacketException {
        this.middle = this.data;
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.DATAPACKET;
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "DataPacket: " + packetRank + ":" + lengthPacket;
    }
    public void updateKey(ChannelBuffer key) {
        this.key = (key == null) ? ChannelBuffers.EMPTY_BUFFER : key;
    }
}
