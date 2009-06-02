/**
 * 
 */
package goldengate.r66.core.protocol;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * This is the Object used in every communication between OpenR66 hosts.
 * Its logic is as follow:<br>
 * <ul>
 * <li>A CONTROL Part: Mandatory
 * <ul>
 * <li>1 byte: type of command</li>
 * <li>2 bytes: length of command header except those 3 first bytes (65536 bytes maximum)</li>
 * <li>length bytes as command specifications</li>
 * </ul></li>
 * <li>A DATA Part: Optional (depends on the CONTROL part)</li>
 * </ul><br>
 * The CONTROL Part could be as follow:<br>
 * <ul>
 * <li>First byte: 1 for authentication or disconnection purpose</li>
 * <li>First byte: 2 for requests not on transfer (status, properties, ...)</li>
 * <li>First byte: 4 for requests on transfers (past or current)</li>
 * <li>First byte: 8 for requests for new transfers</li>
 * <li>First byte: 16 for requests of file transfer as send</li>
 * <li>First byte: 32 for requests of file transfer as receive</li>
 * <li>First byte: 64</li>
 * <li>First byte: 128 for administration purpose</li>
 * </ul><br>
 * Then according to the first byte:<br>
 * <ul>
 * <li>1: 4 bytes for Command name, (length-4) bytes for arguments (generally one string with whitespace), Data block could be not empty</li>
 * <li>2: 4 bytes for Command name, (length-4) bytes for arguments (generally one string with whitespace)</li>
 * <li>4: 4 bytes for Command name, (length-4) bytes for arguments (could be split into 8 bytes for transfer id, 8 bytes for date-time, 4 or 8 bytes for values with a 8 bytes property id, ...)</li>
 * <li>8: 4 bytes for Command name, then (length-4) bytes arguments that should be split into<ul>
 * <li>8 bytes for transfer id from requester,</li> 
 * <li>8 bytes for transfer id from response (null at first time),</li> 
 * <li>x bytes for rule name and a white space,</li>
 * <li>y bytes for filename and a white space,</li>
 * <li>z bytes for information and no or multiple white spaces.</li>
 * </ul>So length = 4+8+8+x+y+z
 * </li>
 * <li>16: 4 bytes for Command name, then arguments that should be split into<ul>
 * <li>1 byte for status -first block, continuous block, last block, normal or compressed, retry-,</li>
 * <li>16 bytes for transfer id (8+8),</li> 
 * <li>4 bytes for rank, </li>
 * <li>4 bytes for data length,</li> 
 * <li>4 bytes for final data length -equal if no compression-,</li>
 * <li>4 bytes for length of padding in case of retry,</li>  
 * <li>16 bytes for MD5</li></ul>
 * So length = 4+1+16+4+4+4+4+16 = 53 bytes<br>
 * In case of retry, the position (starting from 0) in the target is equal to padding length*rank.<br>
 * The Data block has a length of data length. If it is compressed, the uncompressed size is final data length.</li>
 * <li>32: 4 bytes for Command name, then as for value 16, arguments that should be split into<ul>
 * <li>1 byte for status -first block, continuous block, last block, normal or compressed, retry-,</li>
 * <li>16 bytes for transfer id (8+8),</li> 
 * <li>4 bytes for rank, </li>
 * <li>4 bytes for data length,</li> 
 * <li>4 bytes for final data length -equal if no compression-,</li>
 * <li>4 bytes for length of padding in case of retry,</li>  
 * <li>16 bytes for MD5</li></ul>
 * So length = 4+1+16+4+4+4+4+16 = 53 bytes<br>
 * In case of retry, the position (starting from 0) in the target is equal to padding length*rank.<br>
 * The Data block has a length of data length. If it is compressed, the uncompressed size is final data length.</li>
 * <li>128: 4 bytes for Command name, (length-4) bytes for arguments (generally one string with whitespace), Data block could be not empty</li>
 * </ul>
 * 
 * @author frederic bregier
 *
 */
public abstract class Request {
	public static byte CONTROL = 1;
	public static byte GLOBAL = 2;
	public static byte TRANSFER = 4;
	public static byte INITTRF = 8;
	public static byte SENDOP = 0x10;
	public static byte RECVOP = 0x20;
	public static byte ADMIN = 0x40;
	public static byte NOOP = (byte)0x80;
	
	protected byte type;
    protected int headerLength;
    protected String command;
    protected int dataLength;
    protected byte[] data;
    protected ChannelBuffer header;
	/**
	 * @return the type
	 */
	public byte getType() {
		return type;
	}
	/**
	 * @return the headerLength
	 */
	public int getHeaderLength() {
		return headerLength;
	}
	
	/**
	 * @return the dataLength
	 */
	public int getDataLength() {
		return dataLength;
	}
	/**
	 * @return the command
	 */
	public String getCommand() {
		return command;
	}
	/**
	 * @return the data
	 */
	public byte[] getData() {
		return data;
	}
    protected abstract void finalizeHeader();
    public abstract void setData(byte[] data);
    
    public ChannelBuffer toChannelBuffer() {
		this.finalizeHeader();
		ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(
				this.header,ChannelBuffers.wrappedBuffer(this.data));
		return buffer;
	}
    public abstract void fromChannelBuffer(ChannelBuffer buffer);
}
