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

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Set;
import java.util.TreeMap;

import org.waarp.common.digest.FilesystemBasedDigest;
import org.waarp.common.filemonitor.FileMonitor.FileItem;
import org.waarp.common.filemonitor.FileMonitor.FileMonitorInformation;
import org.waarp.common.json.JsonHandler;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.openr66.context.task.AbstractExecJavaTask;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.Messages;
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

	public static TreeMap<String, SpooledInformation> spooledInformationMap = new TreeMap<String, SpooledInformTask.SpooledInformation>();
	
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
			}
			finalValidate("Validated");
		} else {
			// unallowed
			logger.warn("SpooledInformTask not allowed as Java Task: "+fullarg);
			invalid();
		}
	}

	/**
	 * @param detailed
	 * @return
	 */
	public static StringBuilder buildSpooledTable(boolean detailed, String uri) {
		StringBuilder builder = new StringBuilder();
		builder.append("<TABLE BORDER=1><CAPTION><A HREF=");
		builder.append(uri);
		if (detailed) {
			builder.append(Messages.getString("SpooledInformTask.TitleDetailed")); //$NON-NLS-1$
		} else {
			builder.append(Messages.getString("SpooledInformTask.TitleNormal")); //$NON-NLS-1$
		}
		// title first
		builder.append("<TR><TH>");
		builder.append(Messages.getString("SpooledInformTask.0")); //$NON-NLS-1$
		builder.append("</TH><TH>");
		builder.append(Messages.getString("SpooledInformTask.1")); //$NON-NLS-1$
		builder.append("</TH><TH>");
		builder.append(Messages.getString("SpooledInformTask.2")); //$NON-NLS-1$
		builder.append("</TH><TH>");
		builder.append(Messages.getString("SpooledInformTask.3")); //$NON-NLS-1$
		builder.append("</TH><TH>");
		builder.append(Messages.getString("SpooledInformTask.4")); //$NON-NLS-1$
		builder.append("</TH><TH>");
		builder.append(Messages.getString("SpooledInformTask.5")); //$NON-NLS-1$
		builder.append("</TH><TH>");
		builder.append(Messages.getString("SpooledInformTask.6")); //$NON-NLS-1$
		builder.append("</TH><TH>");
		builder.append(Messages.getString("SpooledInformTask.7")); //$NON-NLS-1$
		builder.append("</TH><TH>");
		builder.append(Messages.getString("SpooledInformTask.8")); //$NON-NLS-1$
		builder.append("</TH><TH>");
		builder.append(Messages.getString("SpooledInformTask.9")); //$NON-NLS-1$
		builder.append("</TH></TR>");
		// get current information
		Set<String> names = spooledInformationMap.keySet();
		for (String name : names) {
			// per Name
			synchronized (spooledInformationMap) {
				SpooledInformation inform = spooledInformationMap.get(name);
				builder.append("<TR>");
				builder.append("<TH>");
				builder.append(name.replace(',', ' '));
				builder.append("</TH>");
				builder.append("<TD>");
				builder.append(inform.host);
				builder.append("</TD>");
				long time = inform.lastUpdate.getTime() + Configuration.configuration.TIMEOUTCON;
				if (time + Configuration.configuration.TIMEOUTCON < System.currentTimeMillis()) {
					builder.append("<TD bgcolor=Red>");
				} else if (time < System.currentTimeMillis()) {
					builder.append("<TD bgcolor=Orange>");
				} else {
					builder.append("<TD bgcolor=LightGreen>");
				}
				builder.append(inform.lastUpdate);
				builder.append("</TD>");
				if (inform.fileMonitorInformation != null) {
					builder.append(Messages.getString("SpooledInformTask.AllOk")); //$NON-NLS-1$
					builder.append(inform.fileMonitorInformation.globalok);
					builder.append(Messages.getString("SpooledInformTask.AllError")); //$NON-NLS-1$
					builder.append(inform.fileMonitorInformation.globalerror);
					builder.append(Messages.getString("SpooledInformTask.TodayOk")); //$NON-NLS-1$
					builder.append(inform.fileMonitorInformation.todayok);
					builder.append(Messages.getString("SpooledInformTask.TodayError")); //$NON-NLS-1$
					builder.append(inform.fileMonitorInformation.todayerror);
					builder.append("</TD>");
					builder.append("<TD>");
					builder.append(inform.fileMonitorInformation.elapseTime);
					builder.append("</TD>");
					builder.append("<TD>");
					builder.append(inform.fileMonitorInformation.stopFile);
					builder.append("</TD>");
					builder.append("<TD>");
					builder.append(inform.fileMonitorInformation.statusFile);
					builder.append("</TD>");
					builder.append("<TD>");
					builder.append(inform.fileMonitorInformation.scanSubDir);
					builder.append("</TD>");
					String dirs = "";
					for (File dir : inform.fileMonitorInformation.directories) {
						dirs += dir + "<br>";
					}
					builder.append("<TD>");
					builder.append(dirs);
					builder.append("</TD>");
					if (detailed && inform.fileMonitorInformation.fileItems != null) {
						builder.append("<TD><TABLE BORDER=1><TR><TH>");
						builder.append(Messages.getString("SpooledInformTask.10")); //$NON-NLS-1$
						builder.append("</TH><TH>");
						builder.append(Messages.getString("SpooledInformTask.11")); //$NON-NLS-1$
						builder.append("</TH><TH>");
						builder.append(Messages.getString("SpooledInformTask.12")); //$NON-NLS-1$
						builder.append("</TH><TH>");
						builder.append(Messages.getString("SpooledInformTask.13")); //$NON-NLS-1$
						builder.append("</TH><TH>");
						builder.append(Messages.getString("SpooledInformTask.14")); //$NON-NLS-1$
						builder.append("</TH><TH>");
						builder.append(Messages.getString("SpooledInformTask.15")); //$NON-NLS-1$
						builder.append("</TH></TR>");
						for (FileItem fileItem : inform.fileMonitorInformation.fileItems.values()) {
							builder.append("<TR><TD>");
							builder.append(fileItem.file);
							builder.append("</TD>");
							builder.append("<TD>");
							if (fileItem.hash != null) {
								builder.append(FilesystemBasedDigest.getHex(fileItem.hash));
							}
							builder.append("</TD>");
							builder.append("<TD>");
							if (fileItem.lastTime > 0) {
								builder.append(new Date(fileItem.lastTime));
							}
							builder.append("</TD>");
							builder.append("<TD>");
							if (fileItem.timeUsed > 0) {
								builder.append(new Date(fileItem.timeUsed));
							}
							builder.append("</TD>");
							builder.append("<TD>");
							builder.append(fileItem.used);
							builder.append("</TD>");
							builder.append("<TD>");
							builder.append(fileItem.specialId);
							builder.append("</TD></TR>");
						}
						builder.append("</TABLE></TD>");
					} else {
						// simply print number of files
						builder.append("<TD>");
						if (inform.fileMonitorInformation.fileItems != null) {
							builder.append(inform.fileMonitorInformation.fileItems.size());
						} else {
							builder.append(0);
						}
						builder.append("</TD>");
					}
				}
				builder.append("</TR>");
			}
		}
		builder.append("</TABLE>");
		return builder;
	}
}
