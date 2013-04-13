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
 * Keep Alive class
 * 
 * header = empty middle = way end = empty
 * 
 * @author frederic bregier
 */
public class KeepAlivePacket extends AbstractLocalPacket {
	private static final byte ASKVALIDATE = 0;

	private static final byte ANSWERVALIDATE = 1;

	private byte way;

	/**
	 * @param headerLength
	 * @param middleLength
	 * @param endLength
	 * @param buf
	 * @return the new EndTransferPacket from buffer
	 * @throws OpenR66ProtocolPacketException
	 */
	public static KeepAlivePacket createFromBuffer(int headerLength,
			int middleLength, int endLength, ChannelBuffer buf)
			throws OpenR66ProtocolPacketException {
		if (middleLength != 1) {
			throw new OpenR66ProtocolPacketException("Not enough data");
		}
		byte valid = buf.readByte();
		return new KeepAlivePacket(valid);
	}

	/**
	 * @param valid
	 */
	private KeepAlivePacket(byte valid) {
		way = valid;
	}

	/**
     */
	public KeepAlivePacket() {
		way = ASKVALIDATE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
	 */
	@Override
	public void createEnd() {
		end = ChannelBuffers.EMPTY_BUFFER;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
	 */
	@Override
	public void createHeader() {
		header = ChannelBuffers.EMPTY_BUFFER;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
	 */
	@Override
	public void createMiddle() {
		byte[] newbytes = {
				way };
		middle = ChannelBuffers.wrappedBuffer(newbytes);
	}

	@Override
	public byte getType() {
		return LocalPacketFactory.KEEPALIVEPACKET;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
	 */
	@Override
	public String toString() {
		return "KeepAlivePacket: " + way;
	}

	/**
	 * @return True if this packet is to be validated
	 */
	public boolean isToValidate() {
		return way == ASKVALIDATE;
	}

	/**
	 * Validate the connection
	 */
	public void validate() {
		way = ANSWERVALIDATE;
		header = null;
		middle = null;
		end = null;
	}
}
