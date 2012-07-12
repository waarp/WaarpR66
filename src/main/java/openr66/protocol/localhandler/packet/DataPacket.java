/**
 * This file is part of GoldenGate Project (named also GoldenGate or GG).
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All GoldenGate Project is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 * 
 * GoldenGate is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with GoldenGate . If not,
 * see <http://www.gnu.org/licenses/>.
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.utils.FileUtils;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Data packet
 * 
 * header = packetRank middle = data
 * 
 * @author frederic bregier
 */
public class DataPacket extends AbstractLocalPacket {
	private final int packetRank;

	private final int lengthPacket;

	private final ChannelBuffer data;

	private final ChannelBuffer key;

	/**
	 * @param headerLength
	 * @param middleLength
	 * @param endLength
	 * @param buf
	 * @return the new DataPacket from buffer
	 * @throws OpenR66ProtocolPacketException
	 */
	public static DataPacket createFromBuffer(int headerLength,
			int middleLength, int endLength, ChannelBuffer buf)
			throws OpenR66ProtocolPacketException {
		if (headerLength - 1 <= 0) {
			throw new OpenR66ProtocolPacketException("Not enough data");
		}
		if (middleLength <= 0) {
			throw new OpenR66ProtocolPacketException("Not enough data");
		}
		int packetRank = buf.readInt();
		ChannelBuffer data = buf.readBytes(middleLength);
		ChannelBuffer key = endLength > 0 ? buf.readBytes(endLength)
				: ChannelBuffers.EMPTY_BUFFER;
		return new DataPacket(packetRank, data, key);
	}

	/**
	 * @param packetRank
	 * @param data
	 * @param key
	 */
	public DataPacket(int packetRank, ChannelBuffer data, ChannelBuffer key) {
		this.packetRank = packetRank;
		this.data = data;
		this.key = key == null ? ChannelBuffers.EMPTY_BUFFER : key;
		lengthPacket = data.readableBytes();
	}

	/*
	 * (non-Javadoc)
	 * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
	 */
	@Override
	public void createEnd() throws OpenR66ProtocolPacketException {
		end = key;
	}

	/*
	 * (non-Javadoc)
	 * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
	 */
	@Override
	public void createHeader() throws OpenR66ProtocolPacketException {
		header = ChannelBuffers.buffer(4);
		header.writeInt(packetRank);
	}

	/*
	 * (non-Javadoc)
	 * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
	 */
	@Override
	public void createMiddle() throws OpenR66ProtocolPacketException {
		middle = data;
	}

	@Override
	public byte getType() {
		return LocalPacketFactory.DATAPACKET;
	}

	/*
	 * (non-Javadoc)
	 * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
	 */
	@Override
	public String toString() {
		return "DataPacket: " + packetRank + ":" + lengthPacket;
	}

	/**
	 * @return the packetRank
	 */
	public int getPacketRank() {
		return packetRank;
	}

	/**
	 * @return the lengthPacket
	 */
	public int getLengthPacket() {
		return lengthPacket;
	}

	/**
	 * @return the data
	 */
	public ChannelBuffer getData() {
		return data;
	}

	/**
	 * @return the key
	 */
	public ChannelBuffer getKey() {
		return key;
	}

	/**
	 * 
	 * @return True if the MD5 key is valid (or no key is set)
	 */
	public boolean isKeyValid() {
		if (key == null || key == ChannelBuffers.EMPTY_BUFFER) {
			return true;
		}
		ChannelBuffer newbufkey = FileUtils.getHash(data);
		return ChannelBuffers.equals(key, newbufkey);
	}
}
