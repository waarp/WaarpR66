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
import org.waarp.openr66.protocol.localhandler.packet.InformationPacket;
import org.waarp.openr66.protocol.localhandler.packet.ValidPacket;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;

/**
 * Class to request information on remote files
 * 
 * @author Frederic Bregier
 * 
 */
public class RequestInformation implements Runnable {
	/**
	 * Internal Logger
	 */
	static volatile WaarpInternalLogger logger;

	protected static String _INFO_ARGS = 
			"Needs at least 3 arguments:\n" +
					"  the XML client configuration file,\n" +
					"  '-to' the remoteHost Id,\n" +
					"Other options:\n" +
					"  '-rule' the rule (mandatory for this group of option)\n" +
					"  '-file' the optional file for which to get info,\n" +
					"  '-exist' to test the existence\n" +
					"  '-detail' to get the detail on file\n" +
					"  '-list' to get the list of files\n" +
					"  '-mlsx' to get the list and details of files\n" +
					"  The following option is exclusive of the previous ones:\n" +
					"  '-id' follow by the id and '-reqto (default)|-reqfrom' as additional parameter to get " +
					"the full information of a transfer where current host is requested(to) or requester(from)";
	
	protected final NetworkTransaction networkTransaction;
	final R66Future future;
	String requested = null;
	String filename = null;
	String rulename = null;
	byte code;
	long id = DbConstant.ILLEGALVALUE;
	boolean isTo = true;

	static String srequested = null;
	static String sfilename = null;
	static String srulename = null;
	static byte scode = -1;
	static long sid = DbConstant.ILLEGALVALUE;
	static boolean sisTo = true;

	/**
	 * Parse the parameter and set current values
	 * 
	 * @param args
	 * @return True if all parameters were found and correct
	 */
	protected static boolean getParams(String[] args) {
		if (args.length < 5) {
			logger
					.error(_INFO_ARGS);
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
			} else if (args[i].equalsIgnoreCase("-file")) {
				i++;
				sfilename = args[i];
			} else if (args[i].equalsIgnoreCase("-rule")) {
				i++;
				srulename = args[i];
			} else if (args[i].equalsIgnoreCase("-exist")) {
				scode = (byte) InformationPacket.ASKENUM.ASKEXIST.ordinal();
			} else if (args[i].equalsIgnoreCase("-detail")) {
				scode = (byte) InformationPacket.ASKENUM.ASKMLSDETAIL.ordinal();
			} else if (args[i].equalsIgnoreCase("-list")) {
				scode = (byte) InformationPacket.ASKENUM.ASKLIST.ordinal();
			} else if (args[i].equalsIgnoreCase("-mlsx")) {
				scode = (byte) InformationPacket.ASKENUM.ASKMLSLIST.ordinal();
			} else if (args[i].equalsIgnoreCase("-id")) {
				i++;
				sid = Long.parseLong(args[i]);
			} else if (args[i].equalsIgnoreCase("-reqfrom")) {
				sisTo = true;
			} else if (args[i].equalsIgnoreCase("-reqto")) {
				sisTo = false;
			}
		}
		if (sfilename != null && scode == -1) {
			scode = (byte) InformationPacket.ASKENUM.ASKEXIST.ordinal();
		}
		if ((srulename == null && scode != -1) || srequested == null) {
			logger.error("(Rulename or TransferId) and Requested HostId must be set.\n"+_INFO_ARGS);
			return false;
		}
		if (scode != -1 && sid != DbConstant.ILLEGALVALUE) {
			logger.error("Setting ID is exclusive of other options.\n"+_INFO_ARGS);
			return false;
		}
		return true;
	}

	/**
	 * @param future
	 * @param requested
	 * @param rulename
	 * @param filename
	 * @param request
	 * @param id Id of the request
	 * @param isTo request is To remote Host (true), or From remote host (false)
	 * @param networkTransaction
	 */
	public RequestInformation(R66Future future, String requested, String rulename,
			String filename, byte request, long id, boolean isTo,
			NetworkTransaction networkTransaction) {
		this.future = future;
		this.rulename = rulename;
		this.requested = requested;
		this.filename = filename;
		this.code = request;
		this.id = id;
		this.isTo = isTo;
		this.networkTransaction = networkTransaction;
	}

	public void run() {
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(RequestInformation.class);
		}
		InformationPacket request = null;
		if (code != -1) {
			
			request = new InformationPacket(rulename, code, filename);
		} else {
			request = new InformationPacket(""+id, code, (isTo ? "1" : "0"));
		}

		// Connection
		DbHostAuth host = R66Auth.getServerAuth(DbConstant.admin.session,
				requested);
		if (host == null) {
			logger.error("Requested host cannot be found: " + requested);
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

		LocalChannelReference localChannelReference = networkTransaction
				.createConnectionWithRetry(socketAddress, isSSL, future);
		socketAddress = null;
		if (localChannelReference == null) {
			logger.error("Cannot connect to server: " + requested);
			R66Result result = new R66Result(null, true, ErrorCode.ConnectionImpossible, null);
			this.future.setResult(result);
			this.future.cancel();
			return;
		}
		localChannelReference.sessionNewState(R66FiniteDualStates.INFORMATION);
		try {
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, request, false);
		} catch (OpenR66ProtocolPacketException e) {
			logger.error("Cannot write request");
			R66Result result = new R66Result(null, true, ErrorCode.TransferError, null);
			this.future.setResult(result);
			this.future.cancel();
			return;
		}
		localChannelReference.getFutureRequest().awaitUninterruptibly();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(RequestInformation.class);
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
			RequestInformation requestInformation =
					new RequestInformation(result, srequested, srulename,
							sfilename, scode, sid, sisTo,
							networkTransaction);
			requestInformation.run();
			result.awaitUninterruptibly();
			if (result.isSuccess()) {
				value = 0;
				R66Result r66result = result.getResult();
				ValidPacket info = (ValidPacket) r66result.other;
				logger.warn("SUCCESS     " + info.getSmiddle() + "     " + info.getSheader());
			} else {
				value = 2;
				logger.error("FAILURE     " +
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
