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
package org.waarp.openr66.client;

import java.net.SocketAddress;

import org.jboss.netty.channel.Channels;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.configuration.FileBasedConfiguration;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66FiniteDualStates;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.authentication.R66Auth;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;
import org.waarp.openr66.protocol.localhandler.packet.TestPacket;
import org.waarp.openr66.protocol.localhandler.packet.ValidPacket;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;

/**
 * Message testing between two hosts
 * 
 * @author Frederic Bregier
 * 
 */
public class Message implements Runnable {
	/**
	 * Internal Logger
	 */
	private static WaarpInternalLogger logger;

	final private NetworkTransaction networkTransaction;

	final private R66Future future;

	private final String requested;

	private final DbHostAuth hostAuth;

	final private TestPacket testPacket;

	static String srequested = null;
	static String smessage = "MESSAGE";

	/**
	 * Parse the parameter and set current values
	 * 
	 * @param args
	 * @return True if all parameters were found and correct
	 */
	protected static boolean getParams(String[] args) {
		if (args.length < 5) {
			logger
					.error("Needs 5 arguments:\n" +
							"  the XML client configuration file,\n" +
							"  '-to' the remoteHost Id,\n" +
							"  '-msg' the message\n");
			return false;
		}
		if (!FileBasedConfiguration
				.setClientConfigurationFromXml(Configuration.configuration, args[0])) {
			logger
					.error("Needs a correct configuration file as first argument");
			return false;
		}
		for (int i = 1; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("-to")) {
				i++;
				srequested = args[i];
			} else if (args[i].equalsIgnoreCase("-msg")) {
				i++;
				smessage = args[i];
			}
		}
		if (srequested == null) {
			logger.error("Requested HostId must be set");
			return false;
		}
		return true;
	}

	public Message(NetworkTransaction networkTransaction,
			R66Future future, String requested, TestPacket packet) {
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(Message.class);
		}
		this.networkTransaction = networkTransaction;
		this.future = future;
		this.requested = requested;
		testPacket = packet;
		this.hostAuth = null;
	}

	public Message(NetworkTransaction networkTransaction,
			R66Future future, DbHostAuth hostAuth, TestPacket packet) {
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(Message.class);
		}
		this.networkTransaction = networkTransaction;
		this.future = future;
		this.requested = null;
		testPacket = packet;
		this.hostAuth = hostAuth;
	}

	public void run() {
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(
					Message.class);
		}
		// Connection
		DbHostAuth host = null;
		if (hostAuth == null) {
			host = R66Auth.getServerAuth(DbConstant.admin.session,
					requested);
		} else {
			host = hostAuth;
		}
		if (host == null) {
			logger.debug("Requested host cannot be found: " + requested);
			R66Result result = new R66Result(null, true, ErrorCode.ConnectionImpossible, null);
			this.future.setResult(result);
			this.future.cancel();
			return;
		}
		if (host.isClient()) {
			logger.error("Requested host is a client and cannot be requested: " + requested);
			R66Result result = new R66Result(null, true, ErrorCode.ConnectionImpossible, null);
			this.future.setResult(result);
			this.future.cancel();
			return;
		}
		SocketAddress socketAddress = host.getSocketAddress();
		boolean isSSL = host.isSsl();
		LocalChannelReference localChannelReference = null;
		localChannelReference = networkTransaction
				.createConnectionWithRetry(socketAddress, isSSL, future);
		socketAddress = null;
		if (localChannelReference == null) {
			logger.debug("Cannot connect to server: " + requested);
			R66Result result = new R66Result(null, true, ErrorCode.ConnectionImpossible, null);
			this.future.setResult(result);
			this.future.cancel();
			return;
		}
		localChannelReference.sessionNewState(R66FiniteDualStates.TEST);
		try {
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, testPacket, false);
		} catch (OpenR66ProtocolPacketException e) {
			future.setResult(null);
			future.setFailure(e);
			Channels.close(localChannelReference.getLocalChannel());
			return;
		}
	}

	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(Message.class);
		}
		if (args.length < 5) {
			logger
					.error("Needs 5 arguments:\n" +
							"  the XML client configuration file,\n" +
							"  '-to' the remoteHost Id,\n" +
							"  '-msg' the message\n");
			System.exit(1);
		}
		if (!getParams(args)) {
			logger.error("Wrong initialization");
			if (DbConstant.admin != null && DbConstant.admin.isConnected) {
				DbConstant.admin.close();
			}
			ChannelUtils.stopLogger();
			System.exit(1);
		}
		NetworkTransaction networkTransaction = null;
		int value = 3;
		try {
			Configuration.configuration.pipelineInit();
			networkTransaction = new NetworkTransaction();
			R66Future result = new R66Future(true);
			TestPacket packet = new TestPacket("MSG", smessage, 100);
			Message transaction = new Message(
					networkTransaction, result, srequested,
					packet);
			transaction.run();
			result.awaitUninterruptibly();
			if (result.isSuccess()) {
				value = 0;
				R66Result r66result = result.getResult();
				ValidPacket info = (ValidPacket) r66result.other;
				logger.warn("Test Message\n    SUCCESS\n    " + info.getSheader());
			} else {
				value = 2;
				logger.error("Test Message\n    FAILURE\n    " +
						result.getResult().toString());
			}
		} finally {
			if (networkTransaction != null) {
				networkTransaction.closeAll();
			}
			if (DbConstant.admin != null) {
				DbConstant.admin.close();
			}
			System.exit(value);
		}
	}

}
