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
package org.waarp.openr66.protocol.localhandler.packet;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.waarp.common.digest.FilesystemBasedDigest.DigestAlgo;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;
import org.waarp.openr66.protocol.utils.FileUtils;

/**
 * Data packet
 * 
 * header = packetRank middle = data end = key
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
		int readerIndex = buf.readerIndex();
		ChannelBuffer key;
		if (endLength > 0) {
			key = buf.slice(readerIndex, endLength);
			buf.skipBytes(endLength);
		} else {
			key = ChannelBuffers.EMPTY_BUFFER;
		}
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

	@Override
	public void createEnd(LocalChannelReference lcr) throws OpenR66ProtocolPacketException {
		end = key;
	}

	@Override
	public void createHeader(LocalChannelReference lcr) throws OpenR66ProtocolPacketException {
		header = ChannelBuffers.buffer(4);
		header.writeInt(packetRank);
	}

	@Override
	public void createMiddle(LocalChannelReference lcr) throws OpenR66ProtocolPacketException {
		middle = data;
	}

	@Override
	public byte getType() {
		return LocalPacketFactory.DATAPACKET;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
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
	 * @return True if the Hashed key is valid (or no key is set)
	 */
	public boolean isKeyValid(DigestAlgo algo) {
		if (key == null || key == ChannelBuffers.EMPTY_BUFFER) {
			return true;
		}
		ChannelBuffer newbufkey = FileUtils.getHash(data, algo);
		return ChannelBuffers.equals(key, newbufkey);
	}
}
