/**
 *
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Information of files class
 *
 * header = "rulename" middle = requestedInfo end = "FILENAME"
 *
 * @author frederic bregier
 */
public class InformationPacket extends AbstractLocalPacket {

    public static enum ASKENUM {
        ASKEXIST, ASKMLSDETAIL, ASKLIST, ASKMLSLIST;
    }

    private final String rulename;

    private final byte requestedInfo;

    private final String filename;

    /**
     * @param headerLength
     * @param middleLength
     * @param endLength
     * @param buf
     * @return the new EndTransferPacket from buffer
     * @throws OpenR66ProtocolPacketException
     */
    public static InformationPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf)
            throws OpenR66ProtocolPacketException {
        if (headerLength - 1 <= 0) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        if (middleLength != 1) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        final byte[] bheader = new byte[headerLength - 1];
        final byte[] bend = new byte[endLength];
        if (headerLength - 1 > 0) {
            buf.readBytes(bheader);
        }
        byte request = buf.readByte();
        if (endLength > 0) {
            buf.readBytes(bend);
        }
        final String sheader = new String(bheader);
        final String send = new String(bend);
        return new InformationPacket(sheader, request, send);
    }

    /**
     * @param rulename
     * @param request
     * @param filename
     */
    public InformationPacket(String rulename, byte request, String filename) {
        this.rulename = rulename;
        this.requestedInfo = request;
        this.filename = filename;
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
     */
    @Override
    public void createEnd() {
        if (filename != null) {
            end = ChannelBuffers.wrappedBuffer(filename.getBytes());
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
     */
    @Override
    public void createHeader() throws OpenR66ProtocolPacketException {
        if (rulename == null) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        header = ChannelBuffers.wrappedBuffer(rulename.getBytes());
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
     */
    @Override
    public void createMiddle() {
        byte[] newbytes = {
                requestedInfo };
        middle = ChannelBuffers.wrappedBuffer(newbytes);
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.INFORMATIONPACKET;
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "InformationPacket: " + requestedInfo + " " + rulename+" "+filename;
    }

    /**
     * @return the requestId
     */
    public byte getRequest() {
        return requestedInfo;
    }

    /**
     * @return the rulename
     */
    public String getRulename() {
        return rulename;
    }

    /**
     * @return the filename
     */
    public String getFilename() {
        if (filename != null) {
            return filename;
        } else {
            return "";
        }
    }
}
