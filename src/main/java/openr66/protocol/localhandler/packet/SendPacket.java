/**
 * 
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Send packet
 * 
 * @author frederic bregier
 */
public class SendPacket extends AbstractLocalPacket {
    private String filename;
    private String rulename;
    private String transferInformation;
    
    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
     */
    @Override
    public void createEnd() throws OpenR66ProtocolPacketException {
        end = ChannelBuffers.wrappedBuffer(send.getBytes());
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
     */
    @Override
    public void createHeader() throws OpenR66ProtocolPacketException {
        header = ChannelBuffers.wrappedBuffer(sheader.getBytes());
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
     */
    @Override
    public void createMiddle() throws OpenR66ProtocolPacketException {
        middle = ChannelBuffers.wrappedBuffer(smiddle.getBytes());
    }

    @Override
    public byte getType() {
        if (Integer.parseInt(send) > 100) {
            return LocalPacketFactory.ERRORPACKET;
        }
        return LocalPacketFactory.TESTPACKET;
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "TestPacket: " + sheader + ":" + smiddle + ":" + send;
    }

    public void update() {
        send = Integer.toString(Integer.parseInt(send) + 1);
        end = null;
    }
}
