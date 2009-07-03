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
 * header = "hostId rulename"
 * middle = "filename mode"
 * end = "fileInformation"
 * 
 * @author frederic bregier
 */
public class RequestPacket extends AbstractLocalPacket {
    private String hostId;
    private String rulename;
    private String filename;
    private int mode;
    private String fileInformation;
    
    /**
     * @param headerLength
     * @param middleLength
     * @param endLength
     * @param buf
     * @return the new RequestPacket from buffer
     * @throws OpenR66ProtocolPacketException
     */
    public static RequestPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf) throws OpenR66ProtocolPacketException {
        if (headerLength-1 <=0) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        if (middleLength <=0) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        final byte[] bheader = new byte[headerLength - 1];
        final byte[] bmiddle = new byte[middleLength];
        final byte[] bend = new byte[endLength];
        if (headerLength-1 > 0)
            buf.readBytes(bheader);
        if (middleLength > 0)
            buf.readBytes(bmiddle);
        if (endLength > 0)
            buf.readBytes(bend);
        final String sheader = new String(bheader);
        final String smiddle = new String(bmiddle);
        final String send = new String(bend);
        final String []aheader = sheader.split(" ");
        final String []amiddle = smiddle.split(" ");
        if (aheader.length != 2 && amiddle.length != 2) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        return new RequestPacket(aheader[0], aheader[1], amiddle[0], 
                Integer.parseInt(amiddle[1]), send);
    }
    
    /**
     * @param hostId
     * @param rulename
     * @param filename
     * @param mode
     * @param fileInformation
     */
    public RequestPacket(String hostId, String rulename, String filename, int mode,
            String fileInformation) {
        this.hostId = hostId;
        this.rulename = rulename;
        this.filename = filename;
        this.mode = mode;
        this.fileInformation = fileInformation;
    }
    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
     */
    @Override
    public void createEnd() throws OpenR66ProtocolPacketException {
        if (this.fileInformation != null) {
            end = ChannelBuffers.wrappedBuffer(this.fileInformation.getBytes());
        }
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
     */
    @Override
    public void createHeader() throws OpenR66ProtocolPacketException {
        if (this.hostId == null || this.rulename == null) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        header = ChannelBuffers.wrappedBuffer(this.hostId.getBytes()," ".getBytes(),
                this.rulename.getBytes());
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
     */
    @Override
    public void createMiddle() throws OpenR66ProtocolPacketException {
        if (this.filename == null || this.mode <= 0) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        middle = ChannelBuffers.wrappedBuffer(this.filename.getBytes()," ".getBytes(),
                Integer.toString(this.mode).getBytes());
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.REQUESTPACKET;
    }

    /*
     * (non-Javadoc)
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "RequestPacket: " + hostId + ":" + rulename+" : "+filename+" : "+mode+" : "+fileInformation;
    }

    /**
     * @return the hostId
     */
    public String getHostId() {
        return hostId;
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
