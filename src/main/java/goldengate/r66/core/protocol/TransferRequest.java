/**
 * 
 */
package goldengate.r66.core.protocol;

import org.jboss.netty.buffer.ChannelBuffers;

/**
 * @author fbregier
 *
 */
public class TransferRequest extends Request {
	{
		type = CONTROL;
	}
	public TransferRequest(String command, String argument) {
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

}
