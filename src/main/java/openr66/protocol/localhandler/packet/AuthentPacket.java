/**
 * 
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Request class
 * 
 * header = "hostId"
 * middle = "key bytes"
 * end = none
 * 
 * @author frederic bregier
 */
public class AuthentPacket extends AbstractLocalPacket {
    private String hostId;
    private byte [] key;
    
    public static AuthentPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf) throws OpenR66ProtocolPacketException {
        if (headerLength-1 <=0) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        if (middleLength <=0) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        final byte[] bheader = new byte[headerLength - 1];
        final byte[] bmiddle = new byte[middleLength];
        if (headerLength-1 > 0)
            buf.readBytes(bheader);
        if (middleLength > 0)
            buf.readBytes(bmiddle);
        final String sheader = new String(bheader);
        return new AuthentPacket(sheader, bmiddle);
    }
    
    public AuthentPacket(String hostId, byte []key) {
        this.hostId = hostId;
        this.key = key;
    }
    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
     */
    @Override
    public void createEnd() throws OpenR66ProtocolPacketException {
        end = ChannelBuffers.EMPTY_BUFFER;
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
     */
    @Override
    public void createHeader() throws OpenR66ProtocolPacketException {
        if (this.hostId == null) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        header = ChannelBuffers.wrappedBuffer(this.hostId.getBytes());
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
     */
    @Override
    public void createMiddle() throws OpenR66ProtocolPacketException {
        if (this.key == null) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        middle = ChannelBuffers.wrappedBuffer(this.key);
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.AUTHENTPACKET;
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "AuthentPacket: " + hostId;
    }

}
