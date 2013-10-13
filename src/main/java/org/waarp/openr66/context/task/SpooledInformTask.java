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
package org.waarp.openr66.context.task;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

import org.waarp.common.filemonitor.FileMonitor.FileMonitorInformation;
import org.waarp.common.json.JsonHandler;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.openr66.context.task.AbstractExecJavaTask;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.packet.BusinessRequestPacket;
import org.waarp.openr66.protocol.utils.ChannelUtils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * Java Task for SpooledDirectory information to the Waarp Server
 * 
 * @author Frederic Bregier
 * 
 */
public class SpooledInformTask extends AbstractExecJavaTask {

	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(SpooledInformTask.class);

	public static HashMap<String, SpooledInformation> spooledInformationMap = new HashMap<String, SpooledInformTask.SpooledInformation>();
	
	public static class SpooledInformation {
		public String host;
		public FileMonitorInformation fileMonitorInformation;
		public Date lastUpdate = new Date();
		/**
		 * @param host
		 * @param fileItemHashMap
		 */
		private SpooledInformation(String host, FileMonitorInformation fileMonitorInformation) {
			this.host = host;
			this.fileMonitorInformation = fileMonitorInformation;
		}
	}
	
	@Override
	public void run() {
		if (callFromBusiness) {
			// Business Request to validate?
			if (isToValidate) {
				try {
					FileMonitorInformation fileMonitorInformation = 
							JsonHandler.mapper.readValue(fullarg, FileMonitorInformation.class);
					String host = this.session.getAuth().getUser();
					synchronized (spooledInformationMap) {
						SpooledInformation old = spooledInformationMap.put(fileMonitorInformation.name, new SpooledInformation(host, fileMonitorInformation));
						if  (old != null && old.fileMonitorInformation != null) {
							if (old.fileMonitorInformation.directories != null) {
								old.fileMonitorInformation.directories.clear();
							}
							if (old.fileMonitorInformation.fileItems != null) {
								old.fileMonitorInformation.fileItems.clear();
							}
							old.fileMonitorInformation = null;
						}
						old = null;
					}
				} catch (JsonParseException e1) {
					logger.warn("Cannot parse SpooledInformation", e1);
				} catch (JsonMappingException e1) {
					logger.warn("Cannot parse SpooledInformation", e1);
				} catch (IOException e1) {
					logger.warn("Cannot parse SpooledInformation", e1);
				}
				BusinessRequestPacket packet =
						new BusinessRequestPacket(this.getClass().getName() + " informed", 0);
				validate(packet);
				try {
					ChannelUtils.writeAbstractLocalPacket(session.getLocalChannelReference(),
							packet, true);
				} catch (OpenR66ProtocolPacketException e) {
				}
				this.status = 0;
				return;
			}
			finalValidate("Validated");
		} else {
			// unallowed
			logger.warn("SpooledInformTask not allowed as Java Task: "+fullarg);
			invalid();
		}
	}
}
