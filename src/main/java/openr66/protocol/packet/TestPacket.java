/**
 * 
 */
package openr66.protocol.packet;

import org.jboss.netty.buffer.ChannelBuffers;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

/**
 * Test class for packet
 * 
 * @author frederic bregier
 * 
 */
public class TestPacket extends AbstractLocalPacket {
    private String sheader = null;

    private String smiddle = null;

    private String send = null;

    public TestPacket(String header, String middle, String end) {
        this.sheader = header;
        this.smiddle = middle;
        this.send = end;
    }

    /*
     * (non-Javadoc)
     * 
     * @see openr66.protocol.packet.AbstractLocalPacket#createEnd()
     */
    @Override
    public void createEnd() throws OpenR66ProtocolPacketException {
        this.end = ChannelBuffers.wrappedBuffer(send.getBytes());
    }

    /*
     * (non-Javadoc)
     * 
     * @see openr66.protocol.packet.AbstractLocalPacket#createHeader()
     */
    @Override
    public void createHeader() throws OpenR66ProtocolPacketException {
        this.header = ChannelBuffers.wrappedBuffer(sheader.getBytes());
    }

    /*
     * (non-Javadoc)
     * 
     * @see openr66.protocol.packet.AbstractLocalPacket#createMiddle()
     */
    @Override
    public void createMiddle() throws OpenR66ProtocolPacketException {
        this.middle = ChannelBuffers.wrappedBuffer(smiddle.getBytes());
    }
    @Override
    public byte getType() {
        if (Integer.parseInt(this.send) > 10000) {
            return LocalPacketFactory.ERRORPACKET;
        }
        return LocalPacketFactory.TESTPACKET;
    }
    /*
     * (non-Javadoc)
     * 
     * @see openr66.protocol.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "TestPacket: " + this.sheader + ":" + this.smiddle + ":" +
                this.send;
    }
    public void update() {
        this.send = Integer.toString(Integer.parseInt(this.send)+1);
        this.end = null;
    }
}
