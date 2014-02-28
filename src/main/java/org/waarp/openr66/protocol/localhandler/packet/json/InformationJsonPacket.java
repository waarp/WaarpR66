/**
   This file is part of Waarp Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All Waarp Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   Waarp is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Waarp .  If not, see <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.protocol.localhandler.packet.json;


/**
 * Information (on request or on filesystem) JSON packet
 * @author "Frederic Bregier"
 *
 */
public class InformationJsonPacket extends JsonPacket {

	protected boolean isIdRequest;
	protected long id;
	protected boolean isTo;
	protected byte request;
	protected String rulename;
	protected String filename;
	/**
	 * @return the isIdRequest
	 */
	public boolean isIdRequest() {
		return isIdRequest;
	}
	/**
	 * @param isIdRequest the isIdRequest to set
	 */
	public void setIdRequest(boolean isIdRequest) {
		this.isIdRequest = isIdRequest;
	}
	/**
	 * @return the id
	 */
	public long getId() {
		return id;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(long id) {
		this.id = id;
	}
	/**
	 * @return the isTo
	 */
	public boolean isTo() {
		return isTo;
	}
	/**
	 * @param isTo the isTo to set
	 */
	public void setTo(boolean isTo) {
		this.isTo = isTo;
	}
	/**
	 * @return the request
	 */
	public byte getRequest() {
		return request;
	}
	/**
	 * @param request the request to set
	 */
	public void setRequest(byte request) {
		this.request = request;
	}
	/**
	 * @return the rulename
	 */
	public String getRulename() {
		return rulename;
	}
	/**
	 * @param rulename the rulename to set
	 */
	public void setRulename(String rulename) {
		this.rulename = rulename;
	}
	/**
	 * @return the filename
	 */
	public String getFilename() {
		return filename;
	}
	/**
	 * @param filename the filename to set
	 */
	public void setFilename(String filename) {
		this.filename = filename;
	}
}
