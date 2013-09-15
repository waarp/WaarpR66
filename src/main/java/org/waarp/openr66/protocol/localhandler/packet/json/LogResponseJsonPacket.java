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
 * @author "Frederic Bregier"
 *
 */
public class LogResponseJsonPacket extends LogJsonPacket {

	protected byte command;
	protected String filename;
	protected long exported, purged;
	/**
	 * @return the command
	 */
	public byte getCommand() {
		return command;
	}
	/**
	 * @param command the command to set
	 */
	public void setCommand(byte command) {
		this.command = command;
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
	/**
	 * @return the exported
	 */
	public long getExported() {
		return exported;
	}
	/**
	 * @param exported the exported to set
	 */
	public void setExported(long exported) {
		this.exported = exported;
	}
	/**
	 * @return the purged
	 */
	public long getPurged() {
		return purged;
	}
	/**
	 * @param purged the purged to set
	 */
	public void setPurged(long purged) {
		this.purged = purged;
	}
	
}
