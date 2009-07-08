/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors. This is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of the License,
 * or (at your option) any later version. This software is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * software; if not, write to the Free Software Foundation, Inc., 51 Franklin
 * St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF site:
 * http://www.fsf.org.
 */
package openr66.protocol.networkhandler.packet;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Network Packet A Packet is composed of one global length field, two Id (4
 * bytes x 2) and a buffer. The first Id is the localId on receive operation and
 * the remoteId on send operation. The second Id is the reverse.
 *
 * @author Frederic Bregier
 */
public class NetworkPacket {
    private final ChannelBuffer buffer;

    private final int remoteId;

    private final int localId;

    private final byte code;

    /**
     * @param localId
     * @param remoteId
     * @param code
     * @param buffer
     */
    public NetworkPacket(int localId, int remoteId, byte code,
            ChannelBuffer buffer) {
        this.remoteId = remoteId;
        this.localId = localId;
        this.code = code;
        this.buffer = buffer;
    }

    /**
     * @return the buffer
     */
    public ChannelBuffer getBuffer() {
        return buffer;
    }

    /**
     * @return the remoteId
     */
    public int getRemoteId() {
        return remoteId;
    }

    /**
     * @return the localId
     */
    public int getLocalId() {
        return localId;
    }

    /**
     * @return the code
     */
    public byte getCode() {
        return code;
    }

    /**
     * @return The corresponding ChannelBuffer
     */
    public ChannelBuffer getNetworkPacket() {
        final ChannelBuffer buf = ChannelBuffers.dynamicBuffer(13);
        buf.writeInt(buffer.readableBytes() + 9);
        buf.writeInt(remoteId);
        buf.writeInt(localId);
        buf.writeByte(code);
        return ChannelBuffers.wrappedBuffer(buf, buffer);
    }

    @Override
    public String toString() {
        return "RId: " + remoteId + " LId: " + localId + " Code: " + code +
                " Length: " + buffer.readableBytes();
    }

}
