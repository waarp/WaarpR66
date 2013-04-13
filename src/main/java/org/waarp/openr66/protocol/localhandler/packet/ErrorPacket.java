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
 * Error Message class for packet
 * 
 * 2 strings+1 error code: sheader,smiddle,code
 * 
 * @author frederic bregier
 */
public class ErrorPacket extends AbstractLocalPacket {
	public static final int IGNORECODE = 0;

	public static final int CLOSECODE = 1;

	public static final int FORWARDCODE = 2;

	public static final int FORWARDCLOSECODE = 3;

	private final String sheader;

	private final String smiddle;

	private final int code;

	/**
	 * @param headerLength
	 * @param middleLength
	 * @param endLength
	 * @param buf
	 * @return the new ErrorPacket from buffer
	 * @throws OpenR66ProtocolPacketException
	 */
	public static ErrorPacket createFromBuffer(int headerLength,
			int middleLength, int endLength, ChannelBuffer buf)
			throws OpenR66ProtocolPacketException {
		final byte[] bheader = new byte[headerLength - 1];
		final byte[] bmiddle = new byte[middleLength];
		if (headerLength - 1 > 0) {
			buf.readBytes(bheader);
		}
		if (middleLength > 0) {
			buf.readBytes(bmiddle);
		}
		if (endLength != 4) {
			throw new OpenR66ProtocolPacketException("Packet not correct");
		}
		return new ErrorPacket(new String(bheader), new String(bmiddle), buf
				.readInt());
	}

	/**
	 * @param header
	 * @param middle
	 * @param code
	 */
	public ErrorPacket(String header, String middle, int code) {
		sheader = header;
		smiddle = middle;
		this.code = code;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
	 */
	@Override
	public void createEnd() throws OpenR66ProtocolPacketException {
		end = ChannelBuffers.buffer(4);
		end.writeInt(code);
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
		return "ErrorPacket:(" + code + ":" + smiddle + ") " + sheader;
	}

	@Override
	public byte getType() {
		return LocalPacketFactory.ERRORPACKET;
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
	 * @return the code
	 */
	public int getCode() {
		return code;
	}

}
