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
 * Validation Message class for packet
 * 
 * 2 strings and one byte: sheader,smiddle,send
 * 
 * @author frederic bregier
 */
public class ValidPacket extends AbstractLocalPacket {
	private final String sheader;

	private String smiddle;

	private final byte send;

	/**
	 * @param headerLength
	 * @param middleLength
	 * @param endLength
	 * @param buf
	 * @return the new ValidPacket from buffer
	 */
	public static ValidPacket createFromBuffer(int headerLength,
			int middleLength, int endLength, ChannelBuffer buf) {
		final byte[] bheader = new byte[headerLength - 1];
		final byte[] bmiddle = new byte[middleLength];
		final byte bend;
		if (headerLength - 1 > 0) {
			buf.readBytes(bheader);
		}
		if (middleLength > 0) {
			buf.readBytes(bmiddle);
		}
		bend = buf.readByte();
		return new ValidPacket(new String(bheader), new String(bmiddle), bend);
	}

	/**
	 * @param header
	 * @param middle
	 * @param end
	 */
	public ValidPacket(String header, String middle, byte end) {
		sheader = header;
		smiddle = middle;
		send = end;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
	 */
	@Override
	public void createEnd() throws OpenR66ProtocolPacketException {
		end = ChannelBuffers.buffer(1);
		end.writeByte(send);
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
	 */
	@Override
	public void createHeader() throws OpenR66ProtocolPacketException {
		if (sheader != null) {
			header = ChannelBuffers.wrappedBuffer(sheader.getBytes());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
	 */
	@Override
	public void createMiddle() throws OpenR66ProtocolPacketException {
		if (smiddle != null) {
			middle = ChannelBuffers.wrappedBuffer(smiddle.getBytes());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
	 */
	@Override
	public String toString() {
		return "ValidPacket: " + sheader + ":" + smiddle + ":" + send;
	}

	@Override
	public byte getType() {
		return LocalPacketFactory.VALIDPACKET;
	}

	/**
	 * @return the sheader
	 */
	public String getSheader() {
		return sheader;
	}

	/**
	 * @return the smiddle
	 */
	public String getSmiddle() {
		return smiddle;
	}

	/**
	 * 
	 * @param smiddle
	 */
	public void setSmiddle(String smiddle) {
		this.smiddle = smiddle;
		middle = null;
	}

	/**
	 * @return the type
	 */
	public byte getTypeValid() {
		return send;
	}

}
