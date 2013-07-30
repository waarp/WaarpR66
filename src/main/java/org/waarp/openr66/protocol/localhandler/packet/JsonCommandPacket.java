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
import org.waarp.common.json.JsonHandler;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Json Command Message class for packet
 * 
 * 2 strings and one byte: request,result,send
 * 
 * @author frederic bregier
 */
public class JsonCommandPacket extends AbstractLocalPacket {
	public static enum STOPorCANCELPACKET {comment, requester, requested, specialid};
	public static enum VALIDPACKET {comment, requester, requested, specialid, restarttime};
	public static enum LOGPACKET {purge, clean, start, stop, startid, stopid, rule, request, statuspending, statustransfer, statusdone, statuserror};
	public static enum CONFEXPORTPACKET {host, rule, business, alias, roles};
	public static enum CONFIMPORTPACKET {purgehost, purgerule, purgebusiness, purgealias, purgeroles, host, rule, business, alias, roles, hostid, ruleid, businessid, aliasid, rolesid};
	public static enum INFORMATIONPACKET {result, nbitems};
	public static enum REQUESTPACKET {comment, filename, filesize};
	public static enum BANDWIDTHPACKET {setter, writeglobal, readglobal, writesession, readsession};
	public static enum REQUESTUSERPACKET {command};
	public static enum RESPONSELOGPACKET {command, filename, exported, purged};
	public static enum RESPONSECONFEXPORTPACKET {command, filehost, filerule, filebusiness, filealias, fileroles};
	public static enum RESPONSECONFIMPORTPACKET {command, purgedhost, purgedrule, purgedbusiness, purgedalias, purgedroles, 
		importedhost, importedrule, importedbusiness, importedalias, importedroles};
	
	private final String request;

	private String result;

	private final byte send;

	/**
	 * @param headerLength
	 * @param middleLength
	 * @param endLength
	 * @param buf
	 * @return the new ValidPacket from buffer
	 */
	public static JsonCommandPacket createFromBuffer(int headerLength,
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
		return new JsonCommandPacket(new String(bheader), new String(bmiddle), bend);
	}

	/**
	 * @param srequest
	 * @param sresult
	 * @param end
	 */
	public JsonCommandPacket(String srequest, String sresult, byte end) {
		request = srequest;
		result = sresult;
		send = end;
	}
	
	/**
	 * @param orequest
	 * @param end
	 */
	public JsonCommandPacket(ObjectNode orequest, byte end) {
		request = JsonHandler.writeAsString(orequest);
		result = null;
		send = end;
	}

	/**
	 * @param orequest
	 * @param sresult
	 * @param end
	 */
	public JsonCommandPacket(ObjectNode orequest, String sresult, byte end) {
		request = JsonHandler.writeAsString(orequest);
		result = sresult;
		send = end;
	}

	@Override
	public void createEnd(LocalChannelReference lcr) throws OpenR66ProtocolPacketException {
		end = ChannelBuffers.buffer(1);
		end.writeByte(send);
	}

	@Override
	public void createHeader(LocalChannelReference lcr) throws OpenR66ProtocolPacketException {
		if (request != null) {
			header = ChannelBuffers.wrappedBuffer(request.getBytes());
		}
	}

	@Override
	public void createMiddle(LocalChannelReference lcr) throws OpenR66ProtocolPacketException {
		if (result != null) {
			middle = ChannelBuffers.wrappedBuffer(result.getBytes());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
	 */
	@Override
	public String toString() {
		return "JsonCommandPacket: " + request + ":" + result + ":" + send;
	}

	@Override
	public byte getType() {
		return LocalPacketFactory.JSONREQUESTPACKET;
	}

	/**
	 * @return the node from request
	 */
	public ObjectNode getNodeRequest() {
		return JsonHandler.getFromString(request);
	}

	/**
	 * 
	 * @param result
	 */
	public void setResult(String result) {
		this.result = result;
		middle = null;
	}

	/**
	 * @return the request
	 */
	public String getRequest() {
		return request;
	}

	/**
	 * @return the result
	 */
	public String getResult() {
		return result;
	}

	/**
	 * @return the type
	 */
	public byte getTypeValid() {
		return send;
	}

}
