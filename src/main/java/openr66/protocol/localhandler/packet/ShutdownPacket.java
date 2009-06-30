/**
 * 
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Error Message class for packet
 * 
 * 1 string: spassword(or key)
 * 
 * @author frederic bregier
 */
public class ShutdownPacket extends AbstractLocalPacket {
    private String spassword = null;

    public static ShutdownPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf) throws OpenR66ProtocolPacketException {
        if (headerLength-1 <=0) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        final byte[] bpassword = new byte[headerLength - 1];
        if (headerLength-1 > 0)
            buf.readBytes(bpassword);
        return new ShutdownPacket(new String(bpassword));
    }
    
    public ShutdownPacket(String spassword) {
        this.spassword = spassword;
    }
    public boolean isShutdownValid() {
        // FIXME XXX fix validation
        return true;
    }
    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
     */
    @Override
    public void createEnd() throws OpenR66ProtocolPacketException {
        this.end = ChannelBuffers.EMPTY_BUFFER;
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
     */
    @Override
    public void createHeader() throws OpenR66ProtocolPacketException {
        if (spassword != null) {
            header = ChannelBuffers.wrappedBuffer(spassword.getBytes());
        }
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
     */
    @Override
    public void createMiddle() throws OpenR66ProtocolPacketException {
        this.middle = ChannelBuffers.EMPTY_BUFFER;
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "ShutdownPacket";
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.SHUTDOWNPACKET;
    }
}
