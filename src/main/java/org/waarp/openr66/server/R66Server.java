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
package org.waarp.openr66.server;

import org.jboss.netty.logging.InternalLoggerFactory;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.configuration.FileBasedConfiguration;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.utils.R66ShutdownHook;

/**
 * R66Server startup main class
 * 
 * @author Frederic Bregier
 */
public class R66Server {
	private static WaarpInternalLogger logger;

	/**
	 * @param args
	 *            as first argument the configuration file
	 * @throws OpenR66ProtocolPacketException
	 */
	public static void main(String[] args)
			throws OpenR66ProtocolPacketException {
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
		logger = WaarpInternalLoggerFactory
				.getLogger(R66Server.class);
		if (args.length < 1) {
			logger
					.error("Needs the configuration file as first argument");
			return;
		}
		if (initialize(args[0])) {
			logger.warn("Server OpenR66 starts for " + Configuration.configuration.HOST_ID);
			System.err.println("Server OpenR66 starts for " + Configuration.configuration.HOST_ID);
		} else {
			logger.error("Cannot start Server OpenR66 for " + Configuration.configuration.HOST_ID);
			System.err.println("Cannot start Server OpenR66 for " + Configuration.configuration.HOST_ID);
			System.exit(1);
		}
	}

	public static boolean initialize(String config) {
		if (logger == null) {
			logger = WaarpInternalLoggerFactory
					.getLogger(R66Server.class);
		}
		if (!FileBasedConfiguration
				.setConfigurationServerFromXml(Configuration.configuration, config)) {
			logger
					.error("Needs a correct configuration file as first argument");
			return false;
		}
		try {
			Configuration.configuration.serverStartup();
		} catch (Throwable e) {
			logger
					.error("Startup of server is in error", e);
			R66ShutdownHook.terminate(false);
			return false;
		}
		return true;
	}
}
