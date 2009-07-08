/**
 *
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Validation of Connection (Hello and back) Message class
 *
 * 1 localId (Integer): localId
 *
 * @author frederic bregier
 */
public class ValidateConnectionPacket extends AbstractLocalPacket {
    private static final byte ASKVALIDATE = 0;

    private static final byte ANSWERVALIDATE = 1;

    private final Integer localId;

    private byte way;

    /**
     * @param headerLength
     * @param middleLength
     * @param endLength
     * @param buf
     * @return the new ValidPacket from buffer
     */
    public static ValidateConnectionPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf) {
        Integer newId = buf.readInt();
        byte valid = buf.readByte();
        return new ValidateConnectionPacket(newId, valid);
    }

    /**
     * @param newId
     * @param valid
     */
    private ValidateConnectionPacket(Integer newId, byte valid) {
        localId = newId;
        way = valid;
    }

    /**
     * @param newId
     */
    public ValidateConnectionPacket(Integer newId) {
        localId = newId;
        way = ASKVALIDATE;
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
        header = ChannelBuffers.buffer(5);
        header.writeInt(localId);
        header.writeByte(way);
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
        return "ValidateConnectionPacket: " + localId + " " + way;
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.VALIDATECONNECTIONPACKET;
    }

    /**
     * @return the localId
     */
    public Integer getLocalId() {
        return localId;
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
    }
}
