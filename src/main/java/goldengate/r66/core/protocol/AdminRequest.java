/**
 * 
 */
package goldengate.r66.core.protocol;

import java.nio.charset.Charset;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * @author fbregier
 */
public class AdminRequest extends Request {
    {
        type = CONTROL;
    }

    public AdminRequest(String command, String argument) {
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

    @Override
    public void fromChannelBuffer(ChannelBuffer buffer) {
        // byte header is already read
        headerLength = buffer.readInt();
        dataLength = buffer.readInt();
        command = buffer.toString(0, headerLength, Charset.defaultCharset()
                .name());
        buffer.skipBytes(headerLength);
        if (dataLength > 0) {
            data = new byte[dataLength];
            buffer.readBytes(data);
        } else {
            data = null;
        }
    }

}
