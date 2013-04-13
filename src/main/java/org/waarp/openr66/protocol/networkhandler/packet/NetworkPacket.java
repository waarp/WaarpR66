/**
 * This file is part of Waarp Project.
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All Waarp Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * 
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with Waarp . If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.protocol.networkhandler.packet;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket;

/**
 * Network Packet A Packet is composed of one global length field, two Id (4 bytes x 2) and a
 * buffer. The first Id is the localId on receive operation and the remoteId on send operation. The
 * second Id is the reverse.
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
	 * @param localId
	 * @param remoteId
	 * @param packet
	 * @throws OpenR66ProtocolPacketException
	 */
	public NetworkPacket(int localId, int remoteId, AbstractLocalPacket packet)
			throws OpenR66ProtocolPacketException {
		this.remoteId = remoteId;
		this.localId = localId;
		code = packet.getType();
		buffer = packet.getLocalPacket();
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
