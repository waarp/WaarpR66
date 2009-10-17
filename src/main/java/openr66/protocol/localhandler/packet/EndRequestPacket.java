/**
 *
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * End of Request class
 *
 * header = Error.code middle = way end = empty
 *
 * @author frederic bregier
 */
public class EndRequestPacket extends AbstractLocalPacket {
    private static final byte ASKVALIDATE = 0;

    private static final byte ANSWERVALIDATE = 1;

    private final int code;

    private byte way;

    /**
     * @param headerLength
     * @param middleLength
     * @param endLength
     * @param buf
     * @return the new EndTransferPacket from buffer
     * @throws OpenR66ProtocolPacketException
     */
    public static EndRequestPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf)
            throws OpenR66ProtocolPacketException {
        if (headerLength - 1 != 4) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        if (middleLength != 1) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        final int bheader = buf.readInt();
        byte valid = buf.readByte();
        return new EndRequestPacket(bheader, valid);
    }

    /**
     * @param code
     * @param valid
     */
    private EndRequestPacket(int code, byte valid) {
        this.code = code;
        way = valid;
    }

    /**
     * @param code
     */
    public EndRequestPacket(int code) {
        this.code = code;
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
        header = ChannelBuffers.buffer(4);
        header.writeInt(code);
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
        return LocalPacketFactory.ENDREQUESTPACKET;
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "EndRequestPacket: " + code + " " + way;
    }

    /**
     * @return the code
     */
    public int getCode() {
        return code;
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
