/**
 * 
 */
package openr66.protocol.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Test class for packet
 * 
 * @author frederic bregier
 */
public class TestPacket extends AbstractLocalPacket {
    private String sheader = null;

    private String smiddle = null;

    private String send = null;

    public TestPacket(String header, String middle, String end) {
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
        end = ChannelBuffers.wrappedBuffer(send.getBytes());
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.packet.AbstractLocalPacket#createHeader()
     */
    @Override
    public void createHeader() throws OpenR66ProtocolPacketException {
        header = ChannelBuffers.wrappedBuffer(sheader.getBytes());
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.packet.AbstractLocalPacket#createMiddle()
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
     * @see openr66.protocol.packet.AbstractLocalPacket#toString()
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
