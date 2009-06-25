/**
 * 
 */
package openr66.protocol.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * This class represents Abstract Packet with its header, middle and end parts.
 * @author frederic bregier
 *
 */
public abstract class AbstractPacket {
	protected ChannelBuffer header;
	protected ChannelBuffer middle;
	protected ChannelBuffer end;
	
	public abstract void createHeader() throws OpenR66ProtocolPacketException;
	public abstract void createMiddle() throws OpenR66ProtocolPacketException;
	public abstract void createEnd() throws OpenR66ProtocolPacketException;
	public abstract void updateHeader() throws OpenR66ProtocolPacketException;
	public abstract void updateMiddle() throws OpenR66ProtocolPacketException;
	public abstract void updateEnd() throws OpenR66ProtocolPacketException;
	
	public ChannelBuffer getPacket() {
		ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer(header, middle, end);
		return channelBuffer;
	}
}
