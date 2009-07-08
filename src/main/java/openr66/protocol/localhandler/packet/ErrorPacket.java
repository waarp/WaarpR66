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
    public static final int IGNORECODE = 0;

    public static final int CLOSECODE = 1;

    public static final int FORWARDCODE = 2;

    public static final int FORWARDCLOSECODE = 3;

    private String sheader = null;

    private String smiddle = null;

    private int code = IGNORECODE;

    /**
     * @param headerLength
     * @param middleLength
     * @param endLength
     * @param buf
     * @return the new ErrorPacket from buffer
     * @throws OpenR66ProtocolPacketException
     */
    public static ErrorPacket createFromBuffer(int headerLength,
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
        if (endLength != 4) {
            throw new OpenR66ProtocolPacketException("Packet not correct");
        }
        return new ErrorPacket(new String(bheader), new String(bmiddle), buf
                .readInt());
    }

    /**
     * @param header
     * @param middle
     * @param code
     */
    public ErrorPacket(String header, String middle, int code) {
        sheader = header;
        smiddle = middle;
        this.code = code;
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
     */
    @Override
    public void createEnd() throws OpenR66ProtocolPacketException {
        end = ChannelBuffers.buffer(4);
        end.writeInt(code);
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
        return "ErrorPacket: " + sheader + ":" + smiddle + ":" + code;
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.ERRORPACKET;
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
     * @return the code
     */
    public int getCode() {
        return code;
    }

}
