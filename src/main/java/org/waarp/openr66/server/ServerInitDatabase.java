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

import java.io.File;

import org.jboss.netty.logging.InternalLoggerFactory;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.configuration.AuthenticationFileBasedConfiguration;
import org.waarp.openr66.configuration.FileBasedConfiguration;
import org.waarp.openr66.configuration.RuleFileBasedConfiguration;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.model.DbModelFactory;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;
import org.waarp.openr66.protocol.utils.ChannelUtils;

/**
 * Utility class to initiate the database for a server
 * 
 * @author Frederic Bregier
 * 
 */
public class ServerInitDatabase {
	/**
	 * Internal Logger
	 */
	static volatile WaarpInternalLogger logger;

	static String sxml = null;
	static boolean database = false;
	static String sdirconfig = null;
	static String shostauth = null;
	static String slimitconfig = null;

	protected static boolean getParams(String[] args) {
		if (args.length < 1) {
			logger.error("Need at least the configuration file as first argument then optionally\n"
					+
					"    -initdb\n" +
					"    -dir directory for rules configuration\n" +
					"    -limit xmlfile containing limit of bandwidth\n" +
					"    -auth xml file containing the authentication of hosts");
			return false;
		}
		sxml = args[0];
		for (int i = 1; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("-initdb")) {
				database = true;
			} else if (args[i].equalsIgnoreCase("-dir")) {
				i++;
				sdirconfig = args[i];
			} else if (args[i].equalsIgnoreCase("-limit")) {
				i++;
				slimitconfig = args[i];
			} else if (args[i].equalsIgnoreCase("-auth")) {
				i++;
				shostauth = args[i];
			}
		}
		return true;
	}

	/**
	 * @param args
	 *            as config_database file [rules_directory host_authent limit_configuration]
	 */
	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(ServerInitDatabase.class);
		}
		if (!getParams(args)) {
			logger.error("Need at least the configuration file as first argument then optionally\n"
					+
					"    -initdb\n" +
					"    -dir directory for rules configuration\n" +
					"    -limit xmlfile containing limit of bandwidth\n" +
					"    -auth xml file containing the authentication of hosts");
			if (DbConstant.admin != null && DbConstant.admin.isConnected) {
				DbConstant.admin.close();
			}
			ChannelUtils.stopLogger();
			System.exit(1);
		}

		try {
			if (!FileBasedConfiguration
					.setConfigurationInitDatabase(Configuration.configuration, args[0])) {
				logger
						.error("Needs a correct configuration file as first argument");
				if (DbConstant.admin != null) {
					DbConstant.admin.close();
				}
				ChannelUtils.stopLogger();
				System.exit(1);
				return;
			}
			if (database) {
				// Init database
				try {
					initdb();
				} catch (WaarpDatabaseNoConnectionException e) {
					logger.error("Cannot connect to database");
					return;
				}
				System.out.println("End creation");
			}
			if (sdirconfig != null) {
				// load Rules
				File dirConfig = new File(sdirconfig);
				if (dirConfig.isDirectory()) {
					loadRules(dirConfig);
				} else {
					System.err.println("Dir is not a directory: " + sdirconfig);
				}
			}
			if (shostauth != null) {
				// Load Host Authentications
				if (args.length > 2) {
					loadHostAuth(shostauth);
				}
			}
			if (slimitconfig != null) {
				// Load configuration
				if (args.length > 2) {
					FileBasedConfiguration.setConfigurationLoadLimitFromXml(
							Configuration.configuration,
							slimitconfig);
				}
			}
			System.out.println("Load done");
		} finally {
			if (DbConstant.admin != null) {
				DbConstant.admin.close();
			}
		}
	}

	public static void initdb() throws WaarpDatabaseNoConnectionException {
		// Create tables: configuration, hosts, rules, runner, cptrunner
		DbModelFactory.dbModel.createTables(DbConstant.admin.session);
	}

	public static void loadRules(File dirConfig) {
		try {
			RuleFileBasedConfiguration.importRules(dirConfig);
		} catch (OpenR66ProtocolSystemException e3) {
			e3.printStackTrace();
		} catch (WaarpDatabaseException e) {
			e.printStackTrace();
		}
	}

	public static void loadHostAuth(String filename) {
		AuthenticationFileBasedConfiguration.loadAuthentication(Configuration.configuration,
				filename);
	}
}
