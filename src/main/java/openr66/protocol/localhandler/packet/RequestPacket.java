/**
 *
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Request class
 *
 * header = "rulename mode" middle = "filename" end = "fileInformation"
 *
 * @author frederic bregier
 */
public class RequestPacket extends AbstractLocalPacket {
    public static final int SENDMODE = 1;
    public static final int RECVMODE = 2;
    public static final int SENDMD5MODE = 3;
    public static final int RECVMD5MODE = 4;

    private final String rulename;

    private final int mode;

    private final String filename;

    private final String fileInformation;

    /**
     * @param headerLength
     * @param middleLength
     * @param endLength
     * @param buf
     * @return the new RequestPacket from buffer
     * @throws OpenR66ProtocolPacketException
     */
    public static RequestPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf)
            throws OpenR66ProtocolPacketException {
        if (headerLength - 1 <= 0) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        if (middleLength <= 0) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        final byte[] bheader = new byte[headerLength - 1];
        final byte[] bmiddle = new byte[middleLength];
        final byte[] bend = new byte[endLength];
        if (headerLength - 1 > 0) {
            buf.readBytes(bheader);
        }
        if (middleLength > 0) {
            buf.readBytes(bmiddle);
        }
        if (endLength > 0) {
            buf.readBytes(bend);
        }
        final String sheader = new String(bheader);
        final String smiddle = new String(bmiddle);
        final String send = new String(bend);
        final String[] aheader = sheader.split(" ");
        if (aheader.length != 2) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        return new RequestPacket(aheader[0], Integer
                .parseInt(aheader[1]), smiddle, send);
    }

    /**
     * @param rulename
     * @param mode
     * @param filename
     * @param fileInformation
     */
    public RequestPacket(String rulename, int mode,
            String filename,
            String fileInformation) {
        this.rulename = rulename;
        this.mode = mode;
        this.filename = filename;
        this.fileInformation = fileInformation;
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
     */
    @Override
    public void createEnd() throws OpenR66ProtocolPacketException {
        if (fileInformation != null) {
            end = ChannelBuffers.wrappedBuffer(fileInformation.getBytes());
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
        if (rulename == null || mode <= 0) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        header = ChannelBuffers.wrappedBuffer(rulename.getBytes(), " "
                .getBytes(), Integer.toString(mode).getBytes());
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
     */
    @Override
    public void createMiddle() throws OpenR66ProtocolPacketException {
        if (filename == null) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        middle = ChannelBuffers.wrappedBuffer(filename.getBytes());
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.REQUESTPACKET;
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "RequestPacket: " + rulename + " : " + mode + " : " + filename +
                " : " + fileInformation;
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
        return filename;
    }

    /**
     * @return the mode
     */
    public int getMode() {
        return mode;
    }

    /**
     * @return the fileInformation
     */
    public String getFileInformation() {
        return fileInformation;
    }

}
