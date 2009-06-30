/**
 * 
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Test class for packet
 * 
 * 3 strings: sheader,smiddle,send
 * 
 * @author frederic bregier
 */
public class TestPacket extends AbstractLocalPacket {
    private String sheader = null;

    private String smiddle = null;

    private String send = null;

    public static TestPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf) {
        final byte[] bheader = new byte[headerLength - 1];
        final byte[] bmiddle = new byte[middleLength];
        final byte[] bend = new byte[endLength];
        if (headerLength-1 > 0)
            buf.readBytes(bheader);
        if (middleLength > 0)
            buf.readBytes(bmiddle);
        if (endLength > 0)
            buf.readBytes(bend);
        return new TestPacket(new String(bheader), new String(bmiddle),
                new String(bend));
    }
    
    public TestPacket(String header, String middle, String end) {
        sheader = header;
        smiddle = middle;
        send = end;
    }

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
