/**
 *
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Startup Message class
 * 
 * 1 localId (Integer): localId
 * 
 * @author frederic bregier
 */
public class StartupPacket extends AbstractLocalPacket {
    private final Integer localId;

    /**
     * @param headerLength
     * @param middleLength
     * @param endLength
     * @param buf
     * @return the new ValidPacket from buffer
     */
    public static StartupPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf) {
        Integer newId = buf.readInt();
        return new StartupPacket(newId);
    }

    /**
     * @param newId
     */
    public StartupPacket(Integer newId) {
        localId = newId;
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
        header = ChannelBuffers.buffer(4);
        header.writeInt(localId);
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
        return "StartupPacket: " + localId;
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.STARTUPPACKET;
    }

    /**
     * @return the localId
     */
    public Integer getLocalId() {
        return localId;
    }

}
