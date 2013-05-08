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
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;

/**
 * Block Request Message class
 * 
 * 1 code (byte as 0: unblock, 1:block): block, 1 string: spassword(or key)
 * 
 * @author frederic bregier
 */
public class BlockRequestPacket extends AbstractLocalPacket {
	private final boolean block;
	private final byte[] key;

	/**
	 * @param headerLength
	 * @param middleLength
	 * @param endLength
	 * @param buf
	 * @return the new ValidPacket from buffer
	 * @throws OpenR66ProtocolPacketException 
	 */
	public static BlockRequestPacket createFromBuffer(int headerLength,
			int middleLength, int endLength, ChannelBuffer buf) throws OpenR66ProtocolPacketException {
		if (headerLength - 2 <= 0) {
			throw new OpenR66ProtocolPacketException("Not enough data");
		}
		byte isblock = buf.readByte();
		final byte[] bpassword = new byte[headerLength - 2];
		if (headerLength - 2 > 0) {
			buf.readBytes(bpassword);
		}
		boolean block = (isblock == 1);
		return new BlockRequestPacket(block, bpassword);
	}

	/**
	 * @param block
	 * @param spassword
	 */
	public BlockRequestPacket(boolean block, byte[] spassword) {
		this.block = block;
		key = spassword;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
	 */
	@Override
	public void createEnd() throws OpenR66ProtocolPacketException {
		end = ChannelBuffers.EMPTY_BUFFER;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
	 */
	@Override
	public void createHeader() throws OpenR66ProtocolPacketException {
		header = ChannelBuffers.buffer(1+key.length);
		header.writeByte(block ? 1 : 0);
		header.writeBytes(key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
	 */
	@Override
	public void createMiddle() throws OpenR66ProtocolPacketException {
		middle = ChannelBuffers.EMPTY_BUFFER;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
	 */
	@Override
	public String toString() {
		return "BlockRequestPacket: " + block;
	}

	@Override
	public byte getType() {
		return LocalPacketFactory.BLOCKREQUESTPACKET;
	}

	/**
	 * @return True if the request is to block new requests, else false
	 */
	public boolean getBlock() {
		return block;
	}

	/**
	 * @return the key
	 */
	public byte[] getKey() {
		return key;
	}
}
