/**
 * 
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Validation Message class for packet
 * 
 * 2 strings and one byte: sheader,smiddle,send
 * 
 * @author frederic bregier
 */
public class ValidPacket extends AbstractLocalPacket {
    private String sheader = null;

    private String smiddle = null;

    private byte send;

    /**
     * @param headerLength
     * @param middleLength
     * @param endLength
     * @param buf
     * @return the new ValidPacket from buffer
     */
    public static ValidPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf) {
        final byte[] bheader = new byte[headerLength - 1];
        final byte[] bmiddle = new byte[middleLength];
        final byte bend;
        if (headerLength-1 > 0)
            buf.readBytes(bheader);
        if (middleLength > 0)
            buf.readBytes(bmiddle);
        bend = buf.readByte();
        return new ValidPacket(new String(bheader), new String(bmiddle),
                bend);
    }
    
    /**
     * @param header
     * @param middle
     * @param end
     */
    public ValidPacket(String header, String middle, byte end) {
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
        end = ChannelBuffers.buffer(1);
        end.writeByte(send);
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
        return "ValidPacket: " + sheader + ":" + smiddle + ":" + send;
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.VALIDPACKET;
    }

    /**
     * @return the sheader
     */
    public String getSheader() {
        return sheader;
    }

    /**
     * @return the smiddle
     */
    public String getSmiddle() {
        return smiddle;
    }

    /**
     * @return the type
     */
    public byte getTypeValid() {
        return send;
    }
    
}
