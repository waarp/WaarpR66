/**
 *
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Request Authentication class
 *
 * header = "hostId" middle = "key bytes" end = localId + way
 *
 * @author frederic bregier
 */
public class AuthentPacket extends AbstractLocalPacket {
    private static final byte ASKVALIDATE = 0;

    private static final byte ANSWERVALIDATE = 1;

    private final Integer localId;

    private byte way;

    private String hostId;

    private byte[] key;

    /**
     * @param headerLength
     * @param middleLength
     * @param endLength
     * @param buf
     * @return the new AuthentPacket from buffer
     * @throws OpenR66ProtocolPacketException
     */
    public static AuthentPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf)
            throws OpenR66ProtocolPacketException {
        if (headerLength - 1 <= 0) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        if (middleLength <= 0) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        if (endLength < 5) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        final byte[] bheader = new byte[headerLength - 1];
        final byte[] bmiddle = new byte[middleLength];
        if (headerLength - 1 > 0) {
            buf.readBytes(bheader);
        }
        if (middleLength > 0) {
            buf.readBytes(bmiddle);
        }
        Integer newId = buf.readInt();
        byte valid = buf.readByte();
        final String sheader = new String(bheader);
        return new AuthentPacket(sheader, bmiddle, newId, valid);
    }

    /**
     * @param hostId
     * @param key
     * @param newId
     * @param valid
     */
    private AuthentPacket(String hostId, byte[] key, Integer newId, byte valid) {
        this.hostId = hostId;
        this.key = key;
        localId = newId;
        way = valid;
    }

    /**
     * @param hostId
     * @param key
     * @param newId
     */
    public AuthentPacket(String hostId, byte[] key, Integer newId) {
        this.hostId = hostId;
        this.key = key;
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
        end = ChannelBuffers.buffer(5);
        end.writeInt(localId);
        end.writeByte(way);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
     */
    @Override
    public void createHeader() throws OpenR66ProtocolPacketException {
        if (hostId == null) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        header = ChannelBuffers.wrappedBuffer(hostId.getBytes());
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
     */
    @Override
    public void createMiddle() throws OpenR66ProtocolPacketException {
        if (key == null) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        middle = ChannelBuffers.wrappedBuffer(key);
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.AUTHENTPACKET;
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "AuthentPacket: " + hostId + " " + localId + " " + way;
    }

    /**
     * @return the hostId
     */
    public String getHostId() {
        return hostId;
    }

    /**
     * @return the key
     */
    public byte[] getKey() {
        return key;
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
    public void validate(boolean isSSL) {
        way = ANSWERVALIDATE;
        hostId = Configuration.configuration.getHostId(isSSL);
        key = Configuration.configuration.HOST_AUTH.getHostkey();
        header = null;
        middle = null;
        end = null;
    }
}
