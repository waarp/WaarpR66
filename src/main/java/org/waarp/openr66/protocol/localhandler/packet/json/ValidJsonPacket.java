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

import java.util.Date;

import org.waarp.openr66.database.DbConstant;


/**
 * @author "Frederic Bregier"
 *
 */
public class ValidJsonPacket extends JsonPacket {

	protected String requester;
	protected String requested;
	protected long specialid = DbConstant.ILLEGALVALUE;
	public Date restarttime;
	/**
	 * @return the requester
	 */
	public String getRequester() {
		return requester;
	}
	/**
	 * @param requester the requester to set
	 */
	public void setRequester(String requester) {
		this.requester = requester;
	}
	/**
	 * @return the requested
	 */
	public String getRequested() {
		return requested;
	}
	/**
	 * @param requested the requested to set
	 */
	public void setRequested(String requested) {
		this.requested = requested;
	}
	/**
	 * @return the specialid
	 */
	public long getSpecialid() {
		return specialid;
	}
	/**
	 * @param specialid the specialid to set
	 */
	public void setSpecialid(long specialid) {
		this.specialid = specialid;
	}
	/**
	 * @return the restarttime
	 */
	public Date getRestarttime() {
		return restarttime;
	}
	/**
	 * @param restarttime the restarttime to set
	 */
	public void setRestarttime(Date restarttime) {
		this.restarttime = restarttime;
	}
	
}
