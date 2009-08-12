/**
 *
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * End of Transfer class
 *
 * header = "request" middle = way end = empty
 *
 * @author frederic bregier
 */
public class EndTransferPacket extends AbstractLocalPacket {
    private static final byte ASKVALIDATE = 0;

    private static final byte ANSWERVALIDATE = 1;

    private final byte request;

    private byte way;

    /**
     * @param headerLength
     * @param middleLength
     * @param endLength
     * @param buf
     * @return the new EndTransferPacket from buffer
     * @throws OpenR66ProtocolPacketException
     */
    public static EndTransferPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf)
            throws OpenR66ProtocolPacketException {
        if (headerLength - 1 != 1) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        if (middleLength != 1) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        final byte bheader = buf.readByte();
        byte valid = buf.readByte();
        return new EndTransferPacket(bheader, valid);
    }

    /**
     * @param request
     * @param valid
     */
    private EndTransferPacket(byte request, byte valid) {
        this.request = request;
        way = valid;
    }

    /**
     * @param request
     */
    public EndTransferPacket(byte request) {
        this.request = request;
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
        byte[] newbytes = {
            request };
        header = ChannelBuffers.wrappedBuffer(newbytes);
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
        return LocalPacketFactory.ENDTRANSFERPACKET;
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "EndTransferPacket: " + request + " " + way;
    }

    /**
     * @return the requestId
     */
    public byte getRequest() {
        return request;
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
