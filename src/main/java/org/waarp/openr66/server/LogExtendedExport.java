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
import java.net.SocketAddress;
import java.sql.Timestamp;

import org.jboss.netty.channel.Channels;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.common.utility.WaarpStringUtils;
import org.waarp.openr66.client.DirectTransfer;
import org.waarp.openr66.configuration.FileBasedConfiguration;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66FiniteDualStates;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;
import org.waarp.openr66.protocol.localhandler.packet.JsonCommandPacket;
import org.waarp.openr66.protocol.localhandler.packet.LocalPacketFactory;
import org.waarp.openr66.protocol.localhandler.packet.json.LogJsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.LogResponseJsonPacket;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;


/**
 * Log Export from a client (local or allowed)
 * 
 * @author Frederic Bregier
 * 
 */
public class LogExtendedExport implements Runnable {
	/**
	 * Internal Logger
	 */
	static volatile WaarpInternalLogger logger;

	protected static String _INFO_ARGS = 
			"Need at least the configuration file as first argument then optionally\n"
			+
			"    -purge\n    -clean\n    -startid id\n    -stopid id\n    -rule rule\n    -request host\n" +
			"    -pending\n    -transfer\n    -done\n    -error\n" +
			"    -start timestamp in format yyyyMMddHHmmssSSS possibly truncated and where one of ':-. ' can be separators\n"
			+
			"    -stop timestamp in same format than start\n" +
			"If not start and no stop are given, stop is Today Midnight (00:00:00)\n" +
			"If start is equals or greater than stop, stop is start+24H\n"+
			"    -host host (optional additional options:\n" +
			"      -ruleDownload ruleDownload if set, will try to download the exported logs\n" +
			"      -import that will try to import exported logs using ruleDownload to download the logs)";
	
	protected final R66Future future;
	protected final boolean clean;
	protected final boolean purgeLog;
	protected final Timestamp start;
	protected final Timestamp stop;
	protected final String startid;
	protected final String stopid;
	protected final String rule;
	protected final String request;
	protected final boolean statuspending;
	protected final boolean statustransfer;
	protected final boolean statusdone;
	protected final boolean statuserror;
	protected String ruleDownload = null;
	protected boolean tryimport = false;
	protected final NetworkTransaction networkTransaction;
	protected DbHostAuth host;

	/**
	 * @param future
	 * @param clean
	 * @param purgeLog
	 * @param start
	 * @param stop
	 * @param startid
	 * @param stopid
	 * @param rule
	 * @param request
	 * @param statuspending
	 * @param statustransfer
	 * @param statusdone
	 * @param statuserror
	 * @param networkTransaction
	 * @param host
	 */
	public LogExtendedExport(R66Future future, boolean clean, boolean purgeLog, Timestamp start,
			Timestamp stop, String startid, String stopid, String rule, String request,
			boolean statuspending, boolean statustransfer, boolean statusdone, boolean statuserror,
			NetworkTransaction networkTransaction, DbHostAuth host) {
		this.future = future;
		this.clean = clean;
		this.purgeLog = purgeLog;
		this.start = start;
		this.stop = stop;
		this.startid = startid;
		this.stopid = stopid;
		this.rule = rule;
		this.request = request;
		this.statuspending = statuspending;
		this.statustransfer = statustransfer;
		this.statusdone = statusdone;
		this.statuserror = statuserror;
		this.networkTransaction = networkTransaction;
		this.host = host;
	}
	
	/**
	 * Try first to download the exported logs, then to import (must be a host different than the source one)
	 * @param ruleDownload
	 * @param tryimport
	 */
	public void setDownloadTryImport(String ruleDownload, boolean tryimport) {
		this.ruleDownload = ruleDownload;
		if (ruleDownload != null && tryimport && 
				this.host.getHostid() != Configuration.configuration.HOST_ID &&
				this.host.getHostid() != Configuration.configuration.HOST_SSLID) {
			this.tryimport = tryimport;
		} else {
			this.tryimport = false;
		}
	}
	
	/**
	 * Prior to call this method, the pipeline and NetworkTransaction must have been initialized. It
	 * is the responsibility of the caller to finish all network resources.
	 */
	public void run() {
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(LogExtendedExport.class);
		}
		byte type = (purgeLog) ? LocalPacketFactory.LOGPURGEPACKET : LocalPacketFactory.LOGPACKET;
		LogJsonPacket node = new LogJsonPacket();
		node.setClean(clean);
		node.setPurge(purgeLog);
		node.setStart(start);
		node.setStop(stop);
		node.setStartid(startid);
		node.setStopid(stopid);
		node.setRule(rule);
		node.setRequest(request);
		node.setStatuspending(statuspending);
		node.setStatustransfer(statustransfer);
		node.setStatuserror(statuserror);
		node.setStatusdone(statusdone);
		
		JsonCommandPacket valid = new JsonCommandPacket(node, type);
		logger.debug("ExtendedLogCommand: " +valid.getRequest());
		SocketAddress socketAddress = host.getSocketAddress();
		boolean isSSL = host.isSsl();

		R66Future newFuture = new R66Future(true);
		LocalChannelReference localChannelReference = networkTransaction
				.createConnectionWithRetry(socketAddress, isSSL, newFuture);
		socketAddress = null;
		if (localChannelReference == null) {
			host = null;
			logger.error("Cannot Connect");
			future.setResult(new R66Result(
					new OpenR66ProtocolNoConnectionException("Cannot connect to server"),
					null, true, ErrorCode.Internal, null));
			future.setFailure(future.getResult().exception);
			return;
		}
		localChannelReference.sessionNewState(R66FiniteDualStates.VALIDOTHER);
		try {
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, valid, false);
		} catch (OpenR66ProtocolPacketException e) {
			logger.error("Bad Protocol", e);
			Channels.close(localChannelReference.getLocalChannel());
			localChannelReference = null;
			host = null;
			valid = null;
			future.setResult(new R66Result(e, null, true,
					ErrorCode.TransferError, null));
			future.setFailure(e);
			return;
		}
		host = null;
		newFuture.awaitUninterruptibly();
		logger.info("Request done with " + (newFuture.isSuccess() ? "success" : "error"));
		if (newFuture.isSuccess() && ruleDownload != null && ! ruleDownload.isEmpty()) {
			try {
				importLog(newFuture);
			} catch (OpenR66ProtocolBusinessException e) {
				Channels.close(localChannelReference.getLocalChannel());
				localChannelReference = null;
				return;
			}
		}
		future.filesize = newFuture.filesize;
		future.runner = newFuture.runner;
		future.setResult(newFuture.getResult());
		if (newFuture.isSuccess()) {
			future.setSuccess();
		} else {
			if (newFuture.getCause() != null) {
				future.setFailure(newFuture.getCause());
			}
			future.cancel();
		}
		Channels.close(localChannelReference.getLocalChannel());
		localChannelReference = null;
	}
	
	public void importLog(R66Future future) throws OpenR66ProtocolBusinessException {
		if (future.isSuccess()) {
			JsonCommandPacket packet = (JsonCommandPacket) future.getResult().other;
			if (packet != null) {
				LogResponseJsonPacket res = (LogResponseJsonPacket) packet.getJsonRequest();
				String fileExported = res.getFilename();
				// download logs
				if (fileExported != null && ! fileExported.isEmpty()) {
					String ruleToExport = ruleDownload;
					R66Future futuretemp = new R66Future(true);
					DirectTransfer transfer = new DirectTransfer(futuretemp, host.getHostid(),
							fileExported,
							ruleToExport, "Get Exported Logs from "
									+ Configuration.configuration.HOST_ID,
							false, Configuration.configuration.BLOCKSIZE,
							DbConstant.ILLEGALVALUE,
							networkTransaction);
					transfer.run();
					File logsFile = null;
					if (!futuretemp.isSuccess()) {
						if (futuretemp.getCause() != null) {
							throw new OpenR66ProtocolBusinessException(futuretemp.getCause());
						}
						throw new OpenR66ProtocolBusinessException("Download of exported logs in error");
					} else {
						logsFile = futuretemp.getResult().file.getTrueFile();
					}
					if (tryimport && DbConstant.admin.isConnected) {
						try {
							DbTaskRunner.loadXml(logsFile);
						} catch (OpenR66ProtocolBusinessException e) {
							logger.warn("Cannot load the logs from "+logsFile.getAbsolutePath()
									+" since: "+ e.getMessage());
							throw new OpenR66ProtocolBusinessException("Cannot load the logs from "+logsFile.getAbsolutePath(),
									e);
						}
					}
				} else {
					throw new OpenR66ProtocolBusinessException("Export log with no file result");
				}
			} else {
				throw new OpenR66ProtocolBusinessException("Export log with no file result");
			}
		} else {
			if (future.getCause() != null) {
				throw new OpenR66ProtocolBusinessException(future.getCause());
			}
			throw new OpenR66ProtocolBusinessException("Export log in error");
		}
	}

	protected static boolean sclean = false;
	protected static boolean spurgeLog = false;
	protected static Timestamp sstart = null;
	protected static Timestamp sstop = null;
	protected static String sstartid, sstopid, srule, srequest;
	protected static boolean sstatuspending = false, sstatustransfer = false, sstatusdone = true, sstatuserror = false;
	protected static String stohost = null;
	protected static String sruleDownload = null;
	protected static boolean stryimport = false;
	
	protected static boolean getParams(String[] args) {
		if (args.length < 1) {
			logger.error(_INFO_ARGS);
			return false;
		}
		if (!FileBasedConfiguration
				.setClientConfigurationFromXml(Configuration.configuration, args[0])) {
			logger.error(_INFO_ARGS);
			return false;
		}
		String ssstart = null;
		String ssstop = null;
		for (int i = 1; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("-purge")) {
				spurgeLog = true;
			} else if (args[i].equalsIgnoreCase("-clean")) {
				sclean = true;
			} else if (args[i].equalsIgnoreCase("-start")) {
				i++;
				ssstart = args[i];
			} else if (args[i].equalsIgnoreCase("-stop")) {
				i++;
				ssstop = args[i];
			} else if (args[i].equalsIgnoreCase("-startid")) {
				i++;
				sstartid = args[i];
			} else if (args[i].equalsIgnoreCase("-stopid")) {
				i++;
				sstopid = args[i];
			} else if (args[i].equalsIgnoreCase("-rule")) {
				i++;
				srule = args[i];
			} else if (args[i].equalsIgnoreCase("-request")) {
				i++;
				srequest = args[i];
			} else if (args[i].equalsIgnoreCase("-pending")) {
				sstatuspending = true;
			} else if (args[i].equalsIgnoreCase("-transfer")) {
				sstatustransfer = true;
			} else if (args[i].equalsIgnoreCase("-done")) {
				sstatusdone = true;
			} else if (args[i].equalsIgnoreCase("-error")) {
				sstatuserror = true;
			} else if (args[i].equalsIgnoreCase("-import")) {
				stryimport = true;
			} else if (args[i].equalsIgnoreCase("-host")) {
				i++;
				stohost = args[i];
			} else if (args[i].equalsIgnoreCase("-ruleDownload")) {
				i++;
				sruleDownload = args[i];
			}
		}
		if (ssstart != null) {
			Timestamp tstart = WaarpStringUtils.fixDate(ssstart);
			if (tstart != null) {
				sstart = tstart;
			}
		}
		if (ssstop != null) {
			Timestamp tstop = WaarpStringUtils.fixDate(ssstop, sstart);
			if (tstop != null) {
				sstop = tstop;
			}
		}
		if (ssstart == null && ssstop == null) {
			sstop = WaarpStringUtils.getTodayMidnight();
		}
		if (stohost == null) {
			stryimport = false;
		}
		return true;
	}

	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(LogExtendedExport.class);
		}
		if (!getParams(args)) {
			logger.error("Wrong initialization");
			if (DbConstant.admin != null && DbConstant.admin.isConnected) {
				DbConstant.admin.close();
			}
			System.exit(1);
		}
		long time1 = System.currentTimeMillis();
		DbHostAuth dbhost = null;
		if (stohost != null) {
			try {
				dbhost = new DbHostAuth(DbConstant.admin.session, stohost);
			} catch (WaarpDatabaseException e) {
				logger.error("Wrong initialization");
				if (DbConstant.admin != null && DbConstant.admin.isConnected) {
					DbConstant.admin.close();
				}
				System.exit(2);
			}
		} else {
			dbhost = Configuration.configuration.HOST_SSLAUTH;
			stohost = Configuration.configuration.HOST_SSLID;
		}
		R66Future future = new R66Future(true);

		Configuration.configuration.pipelineInit();
		NetworkTransaction networkTransaction = new NetworkTransaction();
		try {
			LogExtendedExport transaction = new LogExtendedExport(future,
					sclean, spurgeLog, sstart, sstop,
					sstartid, sstopid, srule, srequest, 
					sstatuspending, sstatustransfer, sstatusdone, sstatuserror, 
					networkTransaction, dbhost);
			transaction.setDownloadTryImport(sruleDownload, stryimport);
			transaction.run();
			future.awaitUninterruptibly();
			long time2 = System.currentTimeMillis();
			long delay = time2 - time1;
			R66Result result = future.getResult();
			if (future.isSuccess()) {
				if (result.code == ErrorCode.Warning) {
					logger.warn("WARNED on file:     " +
							(result.other != null ? ((JsonCommandPacket) result.other).getRequest() :
									"no file")
							+ "     delay: " + delay);
				} else {
					logger.warn("SUCCESS on Final file:     " +
							(result.other != null ? ((JsonCommandPacket) result.other).getRequest() :
									"no file")
							+ "     delay: " + delay);
				}
			} else {
				if (result.code == ErrorCode.Warning) {
					logger.warn("LogExtendedExport is     WARNED", future.getCause());
					networkTransaction.closeAll();
					System.exit(result.code.ordinal());
				} else {
					logger.error("LogExtendedExport in     FAILURE", future.getCause());
					networkTransaction.closeAll();
					System.exit(result.code.ordinal());
				}
			}
		} finally {
			networkTransaction.closeAll();
		}
	}

}
