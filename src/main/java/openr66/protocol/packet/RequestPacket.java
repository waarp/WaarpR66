/**
 * 
 */
package openr66.protocol.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Request class
 * 
 * @author frederic bregier
 */
public class RequestPacket extends AbstractLocalPacket {
    private String srequest = null;

    private String sarg = null;

    public RequestPacket(String srequest, String sarg) {
        this.srequest = srequest;
        this.sarg = sarg;
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.packet.AbstractLocalPacket#createEnd()
     */
    @Override
    public void createEnd() throws OpenR66ProtocolPacketException {
        end = ChannelBuffers.wrappedBuffer(srequest.getBytes());
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.packet.AbstractLocalPacket#createHeader()
     */
    @Override
    public void createHeader() throws OpenR66ProtocolPacketException {
        header = ChannelBuffers.wrappedBuffer(sarg.getBytes());
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.packet.AbstractLocalPacket#createMiddle()
     */
    @Override
    public void createMiddle() throws OpenR66ProtocolPacketException {
        middle = ChannelBuffers.EMPTY_BUFFER;
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.REQUESTPACKET;
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "RequestPacket: " + srequest + ":" + sarg;
    }

}
