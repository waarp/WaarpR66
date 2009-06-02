/**
 * 
 */
package goldengate.r66.core.protocol;

import java.nio.charset.Charset;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * @author fbregier
 *
 */
public class AdminRequest extends Request {
	{
		type = CONTROL;
	}
	public AdminRequest(String command, String argument) {
		if (argument != null) {
			this.command = command+" "+argument;
		} else {
			this.command = command;
		}
	}
	/* (non-Javadoc)
	 * @see goldengate.r66.core.protocol.Request#finalizeHeader()
	 */
	@Override
	protected void finalizeHeader() {
		if (header == null) {
			byte []bcommand = this.command.getBytes();
			this.headerLength = bcommand.length;
			header = ChannelBuffers.dynamicBuffer(1+4+4+this.headerLength);
			header.writeByte(this.type);
			header.writeInt(this.headerLength);
			header.writeInt(this.dataLength);
			header.writeBytes(bcommand);
		}
	}

	/* (non-Javadoc)
	 * @see goldengate.r66.core.protocol.Request#setData(byte[])
	 */
	@Override
	public void setData(byte[] data) {
		this.data = data;
		this.dataLength = data.length;
	}
	@Override
	public void fromChannelBuffer(ChannelBuffer buffer) {
		// byte header is already read
		this.headerLength = buffer.readInt();
		this.dataLength = buffer.readInt();
		this.command = buffer.toString(0,this.headerLength,Charset.defaultCharset().name());
		buffer.skipBytes(this.headerLength);
		if (this.dataLength > 0) {
			this.data = new byte[this.dataLength];
			buffer.readBytes(this.data);
		} else {
			this.data = null;
		}
	}

}
