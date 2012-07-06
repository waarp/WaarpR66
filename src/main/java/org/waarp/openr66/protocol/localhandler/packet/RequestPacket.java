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
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;

/**
 * Request class
 * 
 * header = "rulename MODETRANS" middle = way+"FILENAME BLOCKSIZE RANK specialId code" end =
 * "fileInformation"
 * 
 * @author frederic bregier
 */
public class RequestPacket extends AbstractLocalPacket {
	public static enum TRANSFERMODE {
		UNKNOWNMODE, SENDMODE, RECVMODE, SENDMD5MODE, RECVMD5MODE,
		SENDTHROUGHMODE, RECVTHROUGHMODE, SENDMD5THROUGHMODE, RECVMD5THROUGHMODE;
	}

	private static final byte REQVALIDATE = 0;

	private static final byte REQANSWERVALIDATE = 1;

	private final String rulename;

	private final int mode;

	private String filename;

	private final int blocksize;

	private int rank;

	private long specialId;

	private byte way;

	private char code;

	private final String fileInformation;

	/**
	 * 
	 * @param mode
	 * @return the same mode (RECV or SEND) in MD5 version
	 */
	public static int getModeMD5(int mode) {
		switch (mode) {
			case 1:
			case 2:
			case 5:
			case 6:
				return mode + 2;
		}
		return mode;
	}

	/**
	 * 
	 * @param mode
	 * @return true if this mode is a RECV(MD5) mode
	 */
	public static boolean isRecvMode(int mode) {
		return (mode == TRANSFERMODE.RECVMODE.ordinal() ||
				mode == TRANSFERMODE.RECVMD5MODE.ordinal() ||
				mode == TRANSFERMODE.RECVTHROUGHMODE.ordinal() || mode == TRANSFERMODE.RECVMD5THROUGHMODE
					.ordinal());
	}

	/**
	 * 
	 * @param mode
	 * @param isRequested
	 * @return True if this mode is a THROUGH (MD5) mode
	 */
	public static boolean isSendThroughMode(int mode, boolean isRequested) {
		return ((!isRequested && isSendThroughMode(mode)) || (isRequested && isRecvThroughMode(mode)));
	}

	/**
	 * 
	 * @param mode
	 * @return True if this mode is a SEND THROUGH (MD5) mode
	 */
	public static boolean isSendThroughMode(int mode) {
		return (mode == TRANSFERMODE.SENDTHROUGHMODE.ordinal() || mode == TRANSFERMODE.SENDMD5THROUGHMODE
				.ordinal());
	}

	/**
	 * 
	 * @param mode
	 * @param isRequested
	 * @return True if this mode is a THROUGH (MD5) mode
	 */
	public static boolean isRecvThroughMode(int mode, boolean isRequested) {
		return ((!isRequested && isRecvThroughMode(mode)) || (isRequested && isSendThroughMode(mode)));
	}

	/**
	 * 
	 * @param mode
	 * @return True if this mode is a RECV THROUGH (MD5) mode
	 */
	public static boolean isRecvThroughMode(int mode) {
		return (mode == TRANSFERMODE.RECVTHROUGHMODE.ordinal() || mode == TRANSFERMODE.RECVMD5THROUGHMODE
				.ordinal());
	}

	/**
	 * 
	 * @param mode
	 * @return True if this mode is a THROUGH mode (with or without MD5)
	 */
	public static boolean isThroughMode(int mode) {
		return mode >= TRANSFERMODE.SENDTHROUGHMODE.ordinal() &&
				mode <= TRANSFERMODE.RECVMD5THROUGHMODE.ordinal();
	}

	/**
	 * 
	 * @param mode
	 * @return true if this mode is a MD5 mode
	 */
	public static boolean isMD5Mode(int mode) {
		return (mode == TRANSFERMODE.RECVMD5MODE.ordinal() ||
				mode == TRANSFERMODE.SENDMD5MODE.ordinal() ||
				mode == TRANSFERMODE.SENDMD5THROUGHMODE.ordinal() || mode == TRANSFERMODE.RECVMD5THROUGHMODE
					.ordinal());
	}

	/**
	 * 
	 * @param mode1
	 * @param mode2
	 * @return true if both modes are compatible (both send, or both recv)
	 */
	public static boolean isCompatibleMode(int mode1, int mode2) {
		return ((RequestPacket.isRecvMode(mode1) && RequestPacket.isRecvMode(mode2))
		|| ((!RequestPacket.isRecvMode(mode1)) && (!RequestPacket.isRecvMode(mode2))));
	}

	/**
	 * @param headerLength
	 * @param middleLength
	 * @param endLength
	 * @param buf
	 * @return the new RequestPacket from buffer
	 * @throws OpenR66ProtocolPacketException
	 */
	public static RequestPacket createFromBuffer(int headerLength,
			int middleLength, int endLength, ChannelBuffer buf)
			throws OpenR66ProtocolPacketException {
		if (headerLength - 1 <= 0) {
			throw new OpenR66ProtocolPacketException("Not enough data");
		}
		if (middleLength <= 1) {
			throw new OpenR66ProtocolPacketException("Not enough data");
		}
		final byte[] bheader = new byte[headerLength - 1];
		final byte[] bmiddle = new byte[middleLength - 1];
		final byte[] bend = new byte[endLength];
		if (headerLength - 1 > 0) {
			buf.readBytes(bheader);
		}
		byte valid = buf.readByte();
		if (middleLength > 1) {
			buf.readBytes(bmiddle);
		}
		if (endLength > 0) {
			buf.readBytes(bend);
		}
		final String sheader = new String(bheader);
		final String smiddle = new String(bmiddle);
		final String send = new String(bend);
		final String[] aheader = sheader.split(" ");
		if (aheader.length != 2) {
			throw new OpenR66ProtocolPacketException("Not enough data");
		}
		final String[] amiddle = smiddle.split(" ");
		if (amiddle.length != 5) {
			throw new OpenR66ProtocolPacketException("Not enough data");
		}
		int blocksize = Integer.parseInt(amiddle[1]);
		if (blocksize < 100) {
			blocksize = Configuration.configuration.BLOCKSIZE;
		}
		int rank = Integer.parseInt(amiddle[2]);
		long specialId = Long.parseLong(amiddle[3]);
		char code = amiddle[4].charAt(0);
		return new RequestPacket(aheader[0], Integer.parseInt(aheader[1]),
				amiddle[0], blocksize, rank, specialId, valid, send, code);
	}

	/**
	 * @param rulename
	 * @param mode
	 * @param filename
	 * @param blocksize
	 * @param rank
	 * @param specialId
	 * @param valid
	 * @param fileInformation
	 */
	private RequestPacket(String rulename, int mode, String filename,
			int blocksize, int rank, long specialId, byte valid,
			String fileInformation, char code) {
		this.rulename = rulename;
		this.mode = mode;
		this.filename = filename;
		if (blocksize < 100) {
			this.blocksize = Configuration.configuration.BLOCKSIZE;
		} else {
			this.blocksize = blocksize;
		}
		this.rank = rank;
		this.specialId = specialId;
		way = valid;
		this.fileInformation = fileInformation;
		this.code = code;
	}

	/**
	 * @param rulename
	 * @param mode
	 * @param filename
	 * @param blocksize
	 * @param rank
	 * @param specialId
	 * @param fileInformation
	 */
	public RequestPacket(String rulename, int mode, String filename,
			int blocksize, int rank, long specialId, String fileInformation) {
		this(rulename, mode, filename, blocksize, rank, specialId,
				REQVALIDATE, fileInformation, ErrorCode.InitOk.code);
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
	 */
	@Override
	public void createEnd() throws OpenR66ProtocolPacketException {
		if (fileInformation != null) {
			end = ChannelBuffers.wrappedBuffer(fileInformation.getBytes());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
	 */
	@Override
	public void createHeader() throws OpenR66ProtocolPacketException {
		if (rulename == null || mode <= 0) {
			throw new OpenR66ProtocolPacketException("Not enough data");
		}
		header = ChannelBuffers.wrappedBuffer(rulename.getBytes(), " "
				.getBytes(), Integer.toString(mode).getBytes());
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
	 */
	@Override
	public void createMiddle() throws OpenR66ProtocolPacketException {
		if (filename == null) {
			throw new OpenR66ProtocolPacketException("Not enough data");
		}
		byte[] away = new byte[1];
		away[0] = way;
		middle = ChannelBuffers.wrappedBuffer(away, filename.getBytes(), " "
				.getBytes(), Integer.toString(blocksize).getBytes(), " "
				.getBytes(), Integer.toString(rank).getBytes(), " ".getBytes(),
				Long.toString(specialId).getBytes(), " ".getBytes(),
				Character.toString(code).getBytes());
	}

	@Override
	public byte getType() {
		return LocalPacketFactory.REQUESTPACKET;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
	 */
	@Override
	public String toString() {
		return "RequestPacket: " + rulename + " : " + mode + " : " + filename +
				" : " + fileInformation + " : " + blocksize + " : " + rank +
				" : " + way + " : " + code;
	}

	/**
	 * @return the rulename
	 */
	public String getRulename() {
		return rulename;
	}

	/**
	 * @return the filename
	 */
	public String getFilename() {
		return filename;
	}

	/**
	 * @return the mode
	 */
	public int getMode() {
		return mode;
	}

	/**
	 * 
	 * @return True if this packet concerns a Retrieve operation
	 */
	public boolean isRetrieve() {
		return isRecvMode(mode);
	}

	/**
	 * @return the fileInformation
	 */
	public String getFileInformation() {
		return fileInformation;
	}

	/**
	 * @return the blocksize
	 */
	public int getBlocksize() {
		return blocksize;
	}

	/**
	 * @return the rank
	 */
	public int getRank() {
		return rank;
	}

	/**
	 * @param rank
	 *            the rank to set
	 */
	public void setRank(int rank) {
		this.rank = rank;
	}

	/**
	 * @param specialId
	 *            the specialId to set
	 */
	public void setSpecialId(long specialId) {
		this.specialId = specialId;
	}

	/**
	 * @return the specialId
	 */
	public long getSpecialId() {
		return specialId;
	}

	/**
	 * @return True if this packet is to be validated
	 */
	public boolean isToValidate() {
		return way == REQVALIDATE;
	}

	/**
	 * Validate the request
	 */
	public void validate() {
		way = REQANSWERVALIDATE;
		middle = null;
	}

	/**
	 * @param filename
	 *            the filename to set
	 */
	public void setFilename(String filename) {
		this.filename = filename;
	}

	/**
	 * @return the code
	 */
	public char getCode() {
		return code;
	}

	/**
	 * @param code
	 *            the code to set
	 */
	public void setCode(char code) {
		this.code = code;
	}

}
