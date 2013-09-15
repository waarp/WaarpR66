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

import java.net.SocketAddress;

import org.jboss.netty.channel.Channels;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.configuration.FileBasedConfiguration;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66FiniteDualStates;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.PartnerConfiguration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;
import org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket;
import org.waarp.openr66.protocol.localhandler.packet.JsonCommandPacket;
import org.waarp.openr66.protocol.localhandler.packet.LocalPacketFactory;
import org.waarp.openr66.protocol.localhandler.packet.ValidPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.ConfigExportJsonPacket;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;


/**
 * Config Export from a local client without database connection
 * 
 * @author Frederic Bregier
 * 
 */
public class ConfigExport implements Runnable {
	/**
	 * Internal Logger
	 */
	static volatile WaarpInternalLogger logger;

	protected static String _INFO_ARGS = "Need at least the configuration file as first argument then at least one from\n"
			+
			"    -hosts\n" +
			"    -rules\n"+
			"    -business (if compatible)\n"+
			"    -alias (if compatible)\n"+
			"    -role (if compatible)\n"+
			"    -host host (optional)";
	
	protected final R66Future future;
	protected final boolean host;
	protected final boolean rule;
	protected final boolean business;
	protected final boolean alias;
	protected final boolean role;
	protected final NetworkTransaction networkTransaction;
	protected DbHostAuth dbhost;

	public ConfigExport(R66Future future, boolean host, boolean rule,
			NetworkTransaction networkTransaction) {
		this.future = future;
		this.host = host;
		this.rule = rule;
		this.business = false;
		this.alias = false;
		this.role = false;
		this.networkTransaction = networkTransaction;
		this.dbhost = Configuration.configuration.HOST_SSLAUTH;
	}

	public ConfigExport(R66Future future, boolean host, boolean rule,
			boolean business, boolean alias, boolean role,
			NetworkTransaction networkTransaction) {
		this.future = future;
		this.host = host;
		this.rule = rule;
		this.business = business;
		this.alias = alias;
		this.role = role;
		this.networkTransaction = networkTransaction;
		this.dbhost = Configuration.configuration.HOST_SSLAUTH;
	}
	
	public void setHost(DbHostAuth host) {
		this.dbhost = host;
	}

	/**
	 * Prior to call this method, the pipeline and NetworkTransaction must have been initialized. It
	 * is the responsibility of the caller to finish all network resources.
	 */
	public void run() {
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(ConfigExport.class);
		}
		SocketAddress socketAddress = dbhost.getSocketAddress();
		boolean isSSL = dbhost.isSsl();

		LocalChannelReference localChannelReference = networkTransaction
				.createConnectionWithRetry(socketAddress, isSSL, future);
		socketAddress = null;
		if (localChannelReference == null) {
			dbhost = null;
			logger.error("Cannot Connect");
			future.setResult(new R66Result(
					new OpenR66ProtocolNoConnectionException("Cannot connect to server"),
					null, true, ErrorCode.Internal, null));
			future.setFailure(future.getResult().exception);
			return;
		}
		localChannelReference.sessionNewState(R66FiniteDualStates.VALIDOTHER);
		AbstractLocalPacket valid = null;
		boolean useJson = PartnerConfiguration.useJson(dbhost.getHostid());
		logger.debug("UseJson: "+useJson);
		if (useJson) {
			ConfigExportJsonPacket node = new ConfigExportJsonPacket();
			node.setHost(host);
			node.setRule(rule);
			node.setBusiness(business);
			node.setAlias(alias);
			node.setRoles(role);
			valid = new JsonCommandPacket(node, LocalPacketFactory.CONFEXPORTPACKET);
		} else {
			valid = new ValidPacket(Boolean.toString(host), Boolean.toString(rule),
				LocalPacketFactory.CONFEXPORTPACKET);
		}
		try {
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, valid, false);
		} catch (OpenR66ProtocolPacketException e) {
			logger.error("Bad Protocol", e);
			Channels.close(localChannelReference.getLocalChannel());
			localChannelReference = null;
			dbhost = null;
			valid = null;
			future.setResult(new R66Result(e, null, true,
					ErrorCode.TransferError, null));
			future.setFailure(e);
			return;
		}
		dbhost = null;
		future.awaitUninterruptibly();
		String sresult = "no information";
		if (future.isSuccess() && future.getResult() != null && future.getResult().other != null) {
			sresult = (useJson ? ((JsonCommandPacket) future.getResult().other).getRequest() : 
				((ValidPacket) future.getResult().other).toString());
		}
		logger.info("Config Export done with " + (future.isSuccess() ? "success" : "error") + " ("+sresult+")");
		Channels.close(localChannelReference.getLocalChannel());
		localChannelReference = null;
	}

	protected static boolean shost = false;
	protected static boolean srule = false;
	protected static boolean sbusiness = false;
	protected static boolean salias = false;
	protected static boolean srole = false;
	protected static String stohost = null;
	
	protected static boolean getParams(String[] args) {
		if (args.length < 2) {
			logger.error(_INFO_ARGS);
			return false;
		}
		if (!FileBasedConfiguration
				.setClientConfigurationFromXml(Configuration.configuration, args[0])) {
			logger.error(_INFO_ARGS);
			return false;
		}
		for (int i = 1; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("-hosts")) {
				shost = true;
			} else if (args[i].equalsIgnoreCase("-rules")) {
				srule = true;
			} else if (args[i].equalsIgnoreCase("-business")) {
				sbusiness = true;
			} else if (args[i].equalsIgnoreCase("-alias")) {
				salias = true;
			} else if (args[i].equalsIgnoreCase("-roles")) {
				srole = true;
			} else if (args[i].equalsIgnoreCase("-host")) {
				i++;
				stohost = args[i];
			}
		}
		if ((!shost) && (!srule)) {
			logger.error("Need at least one of -hosts - rules");
			return false;
		}
		return true;
	}

	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(ConfigExport.class);
		}
		if (!getParams(args)) {
			logger.error("Wrong initialization");
			if (DbConstant.admin != null && DbConstant.admin.isConnected) {
				DbConstant.admin.close();
			}
			System.exit(1);
		}
		long time1 = System.currentTimeMillis();
		R66Future future = new R66Future(true);

		Configuration.configuration.pipelineInit();
		NetworkTransaction networkTransaction = new NetworkTransaction();
		try {
			ConfigExport transaction = new ConfigExport(future,
					shost, srule, sbusiness, salias, srole,
					networkTransaction);
			if (stohost != null) {
				try {
					transaction.setHost(new DbHostAuth(DbConstant.admin.session, stohost));
				} catch (WaarpDatabaseException e) {
					logger.error("COnfigExport in     FAILURE since Host is not found: "+stohost, e);
					networkTransaction.closeAll();
					System.exit(10);
				}
			} else {
				stohost = Configuration.configuration.HOST_SSLID;
			}
			transaction.run();
			future.awaitUninterruptibly();
			long time2 = System.currentTimeMillis();
			long delay = time2 - time1;
			R66Result result = future.getResult();
			if (future.isSuccess()) {
				boolean useJson = PartnerConfiguration.useJson(stohost);
				logger.debug("UseJson: "+useJson);
				String message = null;
				if (useJson) {
					message = (result.other != null ? ((JsonCommandPacket) result.other).getRequest() :
							"no file");
				} else {
					message = (result.other != null ? ((ValidPacket) result.other).getSheader() :
							"no file");
				}
				if (result.code == ErrorCode.Warning) {
					logger.warn("WARNED on files:     " +
							message
							+ "     delay: " + delay);
				} else {
					logger.warn("SUCCESS on Final files:     " +
							message
							+ "     delay: " + delay);
				}
			} else {
				if (result.code == ErrorCode.Warning) {
					logger.warn("ConfigExport is     WARNED", future.getCause());
					networkTransaction.closeAll();
					System.exit(result.code.ordinal());
				} else {
					logger.error("ConfigExport in     FAILURE", future.getCause());
					networkTransaction.closeAll();
					System.exit(result.code.ordinal());
				}
			}
		} finally {
			networkTransaction.closeAll();
		}
	}

}
