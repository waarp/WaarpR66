/**
 *
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Keep Alive class
 *
 * header = empty middle = way end = empty
 *
 * @author frederic bregier
 */
public class KeepAlivePacket extends AbstractLocalPacket {
    private static final byte ASKVALIDATE = 0;

    private static final byte ANSWERVALIDATE = 1;

    private byte way;

    /**
     * @param headerLength
     * @param middleLength
     * @param endLength
     * @param buf
     * @return the new EndTransferPacket from buffer
     * @throws OpenR66ProtocolPacketException
     */
    public static KeepAlivePacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf)
            throws OpenR66ProtocolPacketException {
        if (middleLength != 1) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        byte valid = buf.readByte();
        return new KeepAlivePacket(valid);
    }

    /**
     * @param valid
     */
    private KeepAlivePacket(byte valid) {
        way = valid;
    }

    /**
     */
    public KeepAlivePacket() {
        way = ASKVALIDATE;
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
     */
    @Override
    public void createEnd() {
        end = ChannelBuffers.EMPTY_BUFFER;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
     */
    @Override
    public void createHeader() {
        header = ChannelBuffers.EMPTY_BUFFER;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
     */
    @Override
    public void createMiddle() {
        byte[] newbytes = {
            way };
        middle = ChannelBuffers.wrappedBuffer(newbytes);
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.KEEPALIVEPACKET;
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "KeepAlivePacket: " + way;
    }

    /**
     * @return True if this packet is to be validated
     */
    public boolean isToValidate() {
        return way == ASKVALIDATE;
    }

    /**
     * Validate the connection
     */
    public void validate() {
        way = ANSWERVALIDATE;
        header = null;
        middle = null;
        end = null;
    }
}
