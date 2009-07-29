/**
 *
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Shutdown Message class for packet
 *
 * 1 string: spassword(or key)
 *
 * @author frederic bregier
 */
public class ShutdownPacket extends AbstractLocalPacket {
    private final byte[] key;

    /**
     * @param headerLength
     * @param middleLength
     * @param endLength
     * @param buf
     * @return the new ShutdownPacket from buffer
     * @throws OpenR66ProtocolPacketException
     */
    public static ShutdownPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf)
            throws OpenR66ProtocolPacketException {
        if (headerLength - 1 <= 0) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        final byte[] bpassword = new byte[headerLength - 1];
        if (headerLength - 1 > 0) {
            buf.readBytes(bpassword);
        }
        return new ShutdownPacket(bpassword);
    }

    /**
     * @param spassword
     */
    public ShutdownPacket(byte[] spassword) {
        key = spassword;
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
        if (key != null) {
            header = ChannelBuffers.wrappedBuffer(key);
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
        middle = ChannelBuffers.EMPTY_BUFFER;
    }

    /*
     * (non-Javadoc)
     *
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

    /**
     * @return the key
     */
    public byte[] getKey() {
        return key;
    }

}
