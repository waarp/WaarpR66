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
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.PartnerConfiguration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoSslException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;
import org.waarp.openr66.protocol.utils.R66Versions;

/**
 * Request Authentication class
 * 
 * header = "hostId" middle = "key bytes" end = localId + way + (optional version: could be a JSON on the form version.{})
 * 
 * @author frederic bregier
 */
public class AuthentPacket extends AbstractLocalPacket {
	private static final byte ASKVALIDATE = 0;

	private static final byte ANSWERVALIDATE = 1;

	private final Integer localId;

	private byte way;
	
	private String version;
	
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
		// end part
		Integer newId = buf.readInt();
		byte valid = buf.readByte();
		String version = R66Versions.V2_4_12.getVersion(); // first base reference where it is unacceptable
		if (endLength > 5) {
			// version
			byte [] bversion = new byte[endLength-5];
			buf.readBytes(bversion);
			version = new String(bversion);
		}
		final String sheader = new String(bheader);
		return new AuthentPacket(sheader, bmiddle, newId, valid, version);
	}

	/**
	 * @param hostId
	 * @param key
	 * @param newId
	 * @param valid
	 * @param version
	 */
	private AuthentPacket(String hostId, byte[] key, Integer newId, byte valid, String version) {
		this.hostId = hostId;
		this.key = key;
		localId = newId;
		way = valid;
		Configuration.configuration.versions.put(hostId, new PartnerConfiguration(hostId, version));
		this.version = version;
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
		Configuration.configuration.versions.putIfAbsent(hostId, new PartnerConfiguration(hostId));
		version = Configuration.configuration.versions.get(hostId).toString();
	}

	@Override
	public void createEnd(LocalChannelReference lcr) throws OpenR66ProtocolPacketException {
		end = ChannelBuffers.buffer(5+(version != null ? version.getBytes().length : 0));
		end.writeInt(localId);
		end.writeByte(way);
		if (version != null) {
			end.writeBytes(version.getBytes());
		}
	}

	@Override
	public void createHeader(LocalChannelReference lcr) throws OpenR66ProtocolPacketException {
		if (hostId == null) {
			throw new OpenR66ProtocolPacketException("Not enough data");
		}
		header = ChannelBuffers.wrappedBuffer(hostId.getBytes());
	}

	@Override
	public void createMiddle(LocalChannelReference lcr) throws OpenR66ProtocolPacketException {
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
		return "AuthentPacket: " + hostId + " " + localId + " " + way + " " + version;
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
		DbHostAuth auth = isSSL ? Configuration.configuration.HOST_SSLAUTH : Configuration.configuration.HOST_AUTH;
		try {
			hostId = Configuration.configuration.getHostId(isSSL);
		} catch (OpenR66ProtocolNoSslException e) {
			hostId = Configuration.configuration.HOST_ID;
			auth = Configuration.configuration.HOST_AUTH;
		}
		key = FilesystemBasedDigest.passwdCrypt(auth.getHostkey());
		Configuration.configuration.versions.putIfAbsent(hostId, new PartnerConfiguration(hostId));
		version = Configuration.configuration.versions.get(hostId).toString();
		header = null;
		middle = null;
		end = null;
	}
}
