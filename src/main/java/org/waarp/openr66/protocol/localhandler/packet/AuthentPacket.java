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
import org.waarp.common.digest.FilesystemBasedDigest;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoSslException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;

/**
 * Request Authentication class
 * 
 * header = "hostId" middle = "key bytes" end = localId + way
 * 
 * @author frederic bregier
 */
public class AuthentPacket extends AbstractLocalPacket {
	private static final byte ASKVALIDATE = 0;

	private static final byte ANSWERVALIDATE = 1;

	private final Integer localId;

	private byte way;

	private String hostId;

	private byte[] key;

	/**
	 * @param headerLength
	 * @param middleLength
	 * @param endLength
	 * @param buf
	 * @return the new AuthentPacket from buffer
	 * @throws OpenR66ProtocolPacketException
	 */
	public static AuthentPacket createFromBuffer(int headerLength,
			int middleLength, int endLength, ChannelBuffer buf)
			throws OpenR66ProtocolPacketException {
		if (headerLength - 1 <= 0) {
			throw new OpenR66ProtocolPacketException("Not enough data");
		}
		if (middleLength <= 0) {
			throw new OpenR66ProtocolPacketException("Not enough data");
		}
		if (endLength < 5) {
			throw new OpenR66ProtocolPacketException("Not enough data");
		}
		final byte[] bheader = new byte[headerLength - 1];
		final byte[] bmiddle = new byte[middleLength];
		if (headerLength - 1 > 0) {
			buf.readBytes(bheader);
		}
		if (middleLength > 0) {
			buf.readBytes(bmiddle);
		}
		Integer newId = buf.readInt();
		byte valid = buf.readByte();
		final String sheader = new String(bheader);
		return new AuthentPacket(sheader, bmiddle, newId, valid);
	}

	/**
	 * @param hostId
	 * @param key
	 * @param newId
	 * @param valid
	 */
	private AuthentPacket(String hostId, byte[] key, Integer newId, byte valid) {
		this.hostId = hostId;
		this.key = key;
		localId = newId;
		way = valid;
	}

	/**
	 * @param hostId
	 * @param key
	 * @param newId
	 */
	public AuthentPacket(String hostId, byte[] key, Integer newId) {
		this.hostId = hostId;
		this.key = key;
		localId = newId;
		way = ASKVALIDATE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
	 */
	@Override
	public void createEnd() throws OpenR66ProtocolPacketException {
		end = ChannelBuffers.buffer(5);
		end.writeInt(localId);
		end.writeByte(way);
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
	 */
	@Override
	public void createHeader() throws OpenR66ProtocolPacketException {
		if (hostId == null) {
			throw new OpenR66ProtocolPacketException("Not enough data");
		}
		header = ChannelBuffers.wrappedBuffer(hostId.getBytes());
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
	 */
	@Override
	public void createMiddle() throws OpenR66ProtocolPacketException {
		if (key == null) {
			throw new OpenR66ProtocolPacketException("Not enough data");
		}
		middle = ChannelBuffers.wrappedBuffer(key);
	}

	@Override
	public byte getType() {
		return LocalPacketFactory.AUTHENTPACKET;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
	 */
	@Override
	public String toString() {
		return "AuthentPacket: " + hostId + " " + localId + " " + way;
	}

	/**
	 * @return the hostId
	 */
	public String getHostId() {
		return hostId;
	}

	/**
	 * @return the key
	 */
	public byte[] getKey() {
		return key;
	}

	/**
	 * @return the localId
	 */
	public Integer getLocalId() {
		return localId;
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
	public void validate(boolean isSSL) {
		way = ANSWERVALIDATE;
		try {
			hostId = Configuration.configuration.getHostId(isSSL);
		} catch (OpenR66ProtocolNoSslException e) {
			hostId = Configuration.configuration.HOST_ID;
		}
		key = FilesystemBasedDigest.passwdCrypt(Configuration.configuration.HOST_AUTH.getHostkey());
		header = null;
		middle = null;
		end = null;
	}
}
