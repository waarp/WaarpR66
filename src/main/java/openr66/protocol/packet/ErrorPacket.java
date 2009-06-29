/**
 * 
 */
package openr66.protocol.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Error Message class for packet
 * 
 * @author frederic bregier
 */
public class ErrorPacket extends AbstractLocalPacket {
    private String sheader = null;

    private String smiddle = null;

    private String send = null;

    public ErrorPacket(String header, String middle, String end) {
        sheader = header;
        smiddle = middle;
        send = end;
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.packet.AbstractLocalPacket#createEnd()
     */
    @Override
    public void createEnd() throws OpenR66ProtocolPacketException {
        if (send != null) {
            end = ChannelBuffers.wrappedBuffer(send.getBytes());
        }
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.packet.AbstractLocalPacket#createHeader()
     */
    @Override
    public void createHeader() throws OpenR66ProtocolPacketException {
        if (sheader != null) {
            header = ChannelBuffers.wrappedBuffer(sheader.getBytes());
        }
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.packet.AbstractLocalPacket#createMiddle()
     */
    @Override
    public void createMiddle() throws OpenR66ProtocolPacketException {
        if (smiddle != null) {
            middle = ChannelBuffers.wrappedBuffer(smiddle.getBytes());
        }
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "ErrorPacket: " + sheader + ":" + smiddle + ":" + send;
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.ERRORPACKET;
    }
}
