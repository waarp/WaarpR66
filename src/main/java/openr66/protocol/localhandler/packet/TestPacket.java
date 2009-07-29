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
    public static final int pingpong = 100;

    private final String sheader;

    private final String smiddle;

    private int code = 0;

    public static TestPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf) {
        final byte[] bheader = new byte[headerLength - 1];
        final byte[] bmiddle = new byte[middleLength];
        if (headerLength - 1 > 0) {
            buf.readBytes(bheader);
        }
        if (middleLength > 0) {
            buf.readBytes(bmiddle);
        }
        return new TestPacket(new String(bheader), new String(bmiddle), buf
                .readInt());
    }

    public TestPacket(String header, String middle, int code) {
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
        header = ChannelBuffers.wrappedBuffer(sheader.getBytes());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
     */
    @Override
    public void createMiddle() throws OpenR66ProtocolPacketException {
        middle = ChannelBuffers.wrappedBuffer(smiddle.getBytes());
    }

    @Override
    public byte getType() {
        if (code > pingpong) {
            return LocalPacketFactory.VALIDPACKET;
        }
        return LocalPacketFactory.TESTPACKET;
    }

    /*
     * (non-Javadoc)
     * 
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "TestPacket: " + sheader + ":" + smiddle + ":" + code;
    }

    public void update() {
        code ++;
        end = null;
    }
}
