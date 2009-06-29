/**
 * 
 */
package goldengate.r66.core.protocol;

import org.jboss.netty.buffer.ChannelBuffers;

/**
 * @author fbregier
 */
public class ControlRequest extends Request {
    {
        type = CONTROL;
    }

    public ControlRequest(String command, String argument) {
        if (argument != null) {
            this.command = command + " " + argument;
        } else {
            this.command = command;
        }
    }

    /*
     * (non-Javadoc)
     * @see goldengate.r66.core.protocol.Request#finalizeHeader()
     */
    @Override
    protected void finalizeHeader() {
        if (header == null) {
            final byte[] bcommand = command.getBytes();
            headerLength = bcommand.length;
            header = ChannelBuffers.dynamicBuffer(1 + 4 + 4 + headerLength);
            header.writeByte(type);
            header.writeInt(headerLength);
            header.writeInt(dataLength);
            header.writeBytes(bcommand);
        }
    }

    /*
     * (non-Javadoc)
     * @see goldengate.r66.core.protocol.Request#setData(byte[])
     */
    @Override
    public void setData(byte[] data) {
        this.data = data;
        dataLength = data.length;
    }

}
