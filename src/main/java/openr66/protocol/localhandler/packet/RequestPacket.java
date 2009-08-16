/**
 *
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Request class
 *
 * header = "rulename MODE" middle = way+"FILENAME BLOCKSIZE RANK specialId" end
 * = "fileInformation"
 *
 * @author frederic bregier
 */
public class RequestPacket extends AbstractLocalPacket {
    public static enum TRANSFERMODE {
        UNKNOWNMODE, SENDMODE, RECVMODE, SENDMD5MODE, RECVMD5MODE,
        SENDTHROUGHMODE, RECVTHROUGHMODE, SENDMD5THROUGHMODE, RECVMD5THROUGHMODE;
    }

    private static final byte REQVALIDATE = 0;

    private static final byte REQANSWERVALIDATE = 1;

    private final String rulename;

    private final int mode;

    private String filename;

    private final int blocksize;

    private final int rank;

    private long specialId;

    private byte way;

    private final String fileInformation;

    /**
     *
     * @param mode
     * @return the same mode (RECV or SEND) in MD5 version
     */
    public static int getModeMD5(int mode) {
        return mode+2;
    }
    /**
     *
     * @param mode
     * @return true if this mode is a RECV(MD5) mode
     */
    public static boolean isRecvMode(int mode) {
        return (mode == TRANSFERMODE.RECVMODE.ordinal() ||
                mode == TRANSFERMODE.RECVMD5MODE.ordinal() ||
                mode == TRANSFERMODE.RECVTHROUGHMODE.ordinal() ||
                mode == TRANSFERMODE.RECVMD5THROUGHMODE.ordinal());
    }
    /**
     *
     * @param mode
     * @return True if this mode is a SEND THROUGH (MD5) mode
     */
    public static boolean isSendThroughMode(int mode) {
        return (mode == TRANSFERMODE.SENDTHROUGHMODE.ordinal() ||
                mode == TRANSFERMODE.SENDMD5THROUGHMODE.ordinal());
    }
    /**
    *
    * @param mode
    * @return True if this mode is a RECV THROUGH (MD5) mode
    */
   public static boolean isRecvThroughMode(int mode) {
       return (mode == TRANSFERMODE.RECVTHROUGHMODE.ordinal() ||
               mode == TRANSFERMODE.RECVMD5THROUGHMODE.ordinal());
   }
    /**
     *
     * @param mode
     * @return true if this mode is a MD5 mode
     */
    public static boolean isMD5Mode(int mode) {
        return (mode == TRANSFERMODE.RECVMD5MODE.ordinal() ||
                mode == TRANSFERMODE.SENDMD5MODE.ordinal() ||
                mode == TRANSFERMODE.SENDMD5THROUGHMODE.ordinal() ||
                mode == TRANSFERMODE.RECVMD5THROUGHMODE.ordinal());
    }
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
        if (middleLength <= 1) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        final byte[] bheader = new byte[headerLength - 1];
        final byte[] bmiddle = new byte[middleLength - 1];
        final byte[] bend = new byte[endLength];
        if (headerLength - 1 > 0) {
            buf.readBytes(bheader);
        }
        byte valid = buf.readByte();
        if (middleLength > 1) {
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
        final String[] amiddle = smiddle.split(" ");
        if (amiddle.length != 4) {
            throw new OpenR66ProtocolPacketException("Not enough data");
        }
        int blocksize = Integer.parseInt(amiddle[1]);
        if (blocksize < 100) {
            blocksize = Configuration.configuration.BLOCKSIZE;
        }
        int rank = Integer.parseInt(amiddle[2]);
        long specialId = Long.parseLong(amiddle[3]);
        return new RequestPacket(aheader[0], Integer.parseInt(aheader[1]),
                amiddle[0], blocksize, rank, specialId, valid, send);
    }

    /**
     * @param rulename
     * @param mode
     * @param filename
     * @param blocksize
     * @param rank
     * @param specialId
     * @param valid
     * @param fileInformation
     */
    private RequestPacket(String rulename, int mode, String filename,
            int blocksize, int rank, long specialId, byte valid,
            String fileInformation) {
        this.rulename = rulename;
        this.mode = mode;
        this.filename = filename;
        if (blocksize < 100) {
            this.blocksize = Configuration.configuration.BLOCKSIZE;
        } else {
            this.blocksize = blocksize;
        }
        this.rank = rank;
        this.specialId = specialId;
        way = valid;
        this.fileInformation = fileInformation;
    }

    /**
     * @param rulename
     * @param mode
     * @param filename
     * @param blocksize
     * @param rank
     * @param specialId
     * @param fileInformation
     */
    public RequestPacket(String rulename, int mode, String filename,
            int blocksize, int rank, long specialId, String fileInformation) {
        this(rulename, mode, filename, blocksize, rank, specialId, REQVALIDATE,
                fileInformation);
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
        byte[] away = new byte[1];
        away[0] = way;
        middle = ChannelBuffers.wrappedBuffer(away, filename.getBytes(), " "
                .getBytes(), Integer.toString(blocksize).getBytes(), " "
                .getBytes(), Integer.toString(rank).getBytes(), " ".getBytes(),
                Long.toString(specialId).getBytes());
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
                " : " + fileInformation + " : " + blocksize + " : " + rank +
                " : " + way;
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
     *
     * @return True if this packet concerns a Retrieve operation
     */
    public boolean isRetrieve() {
        return isRecvMode(mode);
    }

    /**
     * @return the fileInformation
     */
    public String getFileInformation() {
        return fileInformation;
    }

    /**
     * @return the blocksize
     */
    public int getBlocksize() {
        return blocksize;
    }

    /**
     * @return the rank
     */
    public int getRank() {
        return rank;
    }

    /**
     * @param specialId
     *            the specialId to set
     */
    public void setSpecialId(long specialId) {
        this.specialId = specialId;
    }

    /**
     * @return the specialId
     */
    public long getSpecialId() {
        return specialId;
    }

    /**
     * @return True if this packet is to be validated
     */
    public boolean isToValidate() {
        return way == REQVALIDATE;
    }

    /**
     * Validate the request
     */
    public void validate() {
        way = REQANSWERVALIDATE;
        middle = null;
    }

    /**
     * @param filename
     *            the filename to set
     */
    public void setFilename(String filename) {
        this.filename = filename;
    }

}
