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
 * 3 strings: sheader,smiddle,send
 * 
 * @author frederic bregier
 */
public class ErrorPacket extends AbstractLocalPacket {
    private String sheader = null;

    private String smiddle = null;

    private String send = null;

    public static ErrorPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf) {
        final byte[] bheader = new byte[headerLength - 1];
        final byte[] bmiddle = new byte[middleLength];
        final byte[] bend = new byte[endLength];
        if (headerLength-1 > 0)
            buf.readBytes(bheader);
        if (middleLength > 0)
            buf.readBytes(bmiddle);
        if (endLength > 0)
            buf.readBytes(bend);
        return new ErrorPacket(new String(bheader), new String(bmiddle),
                new String(bend));
    }
    
    public ErrorPacket(String header, String middle, String end) {
        sheader = header;
        smiddle = middle;
        send = end;
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
     */
    @Override
    public void createEnd() throws OpenR66ProtocolPacketException {
        if (send != null) {
            end = ChannelBuffers.wrappedBuffer(send.getBytes());
        }
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
     */
    @Override
    public void createHeader() throws OpenR66ProtocolPacketException {
        if (sheader != null) {
            header = ChannelBuffers.wrappedBuffer(sheader.getBytes());
        }
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
     */
    @Override
    public void createMiddle() throws OpenR66ProtocolPacketException {
        if (smiddle != null) {
            middle = ChannelBuffers.wrappedBuffer(smiddle.getBytes());
        }
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "ErrorPacket: " + sheader + ":" + smiddle + ":" + send;
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.ERRORPACKET;
    }
}
