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
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;

/**
 * Business Request Message class for packet
 * 
 * 1 string and on integer and one byte:<br>
 * - sheader = full text with class at first place - smiddle = integer - send = byte
 * 
 * @author frederic bregier
 */
public class BusinessRequestPacket extends AbstractLocalPacket {
	private static final byte ASKVALIDATE = 0;

	private static final byte ANSWERVALIDATE = 1;
	private static final byte ANSWERINVALIDATE = 2;

	private String sheader;

	private int delay = 0;

	private byte way;

	public static BusinessRequestPacket createFromBuffer(int headerLength,
			int middleLength, int endLength, ChannelBuffer buf)
			throws OpenR66ProtocolPacketException {
		final byte[] bheader = new byte[headerLength - 1];
		if (headerLength - 1 > 0) {
			buf.readBytes(bheader);
		}
		if (middleLength != 4) {
			throw new OpenR66ProtocolPacketException("Packet not correct");
		}
		int delay = buf.readInt();
		if (endLength != 1) {
			throw new OpenR66ProtocolPacketException("Packet not correct");
		}
		byte valid = buf.readByte();
		return new BusinessRequestPacket(new String(bheader), delay, valid);
	}

	public BusinessRequestPacket(String header, int delay, byte way) {
		this.sheader = header;
		this.delay = delay;
		this.way = way;
	}

	public BusinessRequestPacket(String header, int delay) {
		this.sheader = header;
		this.delay = delay;
		this.way = ASKVALIDATE;
	}

	@Override
	public void createEnd(LocalChannelReference lcr) throws OpenR66ProtocolPacketException {
		end = ChannelBuffers.buffer(1);
		end.writeByte(way);
	}

	@Override
	public void createHeader(LocalChannelReference lcr) throws OpenR66ProtocolPacketException {
		header = ChannelBuffers.wrappedBuffer(sheader.getBytes());
	}

	@Override
	public void createMiddle(LocalChannelReference lcr) throws OpenR66ProtocolPacketException {
		middle = ChannelBuffers.buffer(4);
		middle.writeInt(delay);
	}

	@Override
	public byte getType() {
		return LocalPacketFactory.BUSINESSREQUESTPACKET;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
	 */
	@Override
	public String toString() {
		return "BusinessRequestPacket: " + sheader + ":" + delay + ":" + way;
	}

	/**
	 * @return True if this packet is to be validated
	 */
	public boolean isToValidate() {
		return way == ASKVALIDATE;
	}

	/**
	 * Validate the request
	 */
	public void validate() {
		way = ANSWERVALIDATE;
		header = null;
		middle = null;
		end = null;
	}

	/**
	 * Invalidate the request
	 */
	public void invalidate() {
		way = ANSWERINVALIDATE;
		header = null;
		middle = null;
		end = null;
	}

	/**
	 * @return the sheader
	 */
	public String getSheader() {
		return sheader;
	}

	/**
	 * @return the delay
	 */
	public int getDelay() {
		return delay;
	}

	/**
	 * @param delay
	 *            the delay to set
	 */
	public void setDelay(int delay) {
		this.delay = delay;
		middle = null;
	}
}
