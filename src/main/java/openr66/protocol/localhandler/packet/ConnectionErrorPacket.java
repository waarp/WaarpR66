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
public class ConnectionErrorPacket extends AbstractLocalPacket {

    private final String sheader;

    private final String smiddle;

    /**
     * @param headerLength
     * @param middleLength
     * @param endLength
     * @param buf
     * @return the new ErrorPacket from buffer
     * @throws OpenR66ProtocolPacketException
     */
    public static ConnectionErrorPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf)
            throws OpenR66ProtocolPacketException {
        final byte[] bheader = new byte[headerLength - 1];
        final byte[] bmiddle = new byte[middleLength];
        if (headerLength - 1 > 0) {
            buf.readBytes(bheader);
        }
        if (middleLength > 0) {
            buf.readBytes(bmiddle);
        }
        return new ConnectionErrorPacket(new String(bheader), new String(
                bmiddle));
    }

    /**
     * @param header
     * @param middle
     */
    public ConnectionErrorPacket(String header, String middle) {
        sheader = header;
        smiddle = middle;
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
     */
    @Override
    public void createEnd() throws OpenR66ProtocolPacketException {
        end = ChannelBuffers.EMPTY_BUFFER;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
     */
    @Override
    public void createHeader() throws OpenR66ProtocolPacketException {
        if (sheader != null) {
            header = ChannelBuffers.wrappedBuffer(sheader.getBytes());
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
     */
    @Override
    public void createMiddle() throws OpenR66ProtocolPacketException {
        if (smiddle != null) {
            middle = ChannelBuffers.wrappedBuffer(smiddle.getBytes());
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "ConnectionErrorPacket: " + sheader + ":" + smiddle;
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.CONNECTERRORPACKET;
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
}
