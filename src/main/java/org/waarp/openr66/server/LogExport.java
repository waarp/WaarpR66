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
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;

import org.jboss.netty.channel.Channels;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.configuration.FileBasedConfiguration;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66FiniteDualStates;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;
import org.waarp.openr66.protocol.localhandler.packet.LocalPacketFactory;
import org.waarp.openr66.protocol.localhandler.packet.ValidPacket;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;

/**
 * Log Export from a local client without database connection
 * 
 * @author Frederic Bregier
 * 
 */
public class LogExport implements Runnable {
	/**
	 * Internal Logger
	 */
	static volatile WaarpInternalLogger logger;

	protected final R66Future future;
	protected final boolean purgeLog;
	protected final Timestamp start;
	protected final Timestamp stop;
	protected final boolean clean;
	protected final NetworkTransaction networkTransaction;

	public LogExport(R66Future future, boolean purgeLog, boolean clean,
			Timestamp start, Timestamp stop,
			NetworkTransaction networkTransaction) {
		this.future = future;
		this.purgeLog = purgeLog;
		this.clean = clean;
		this.start = start;
		this.stop = stop;
		this.networkTransaction = networkTransaction;
	}

	/**
	 * Prior to call this method, the pipeline and NetworkTransaction must have been initialized. It
	 * is the responsibility of the caller to finish all network resources.
	 */
	public void run() {
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(LogExport.class);
		}
		String lstart = (start != null) ? start.toString() : null;
		String lstop = (stop != null) ? stop.toString() : null;
		byte type = (purgeLog) ? LocalPacketFactory.LOGPURGEPACKET : LocalPacketFactory.LOGPACKET;
		ValidPacket valid = new ValidPacket(lstart, lstop, type);
		DbHostAuth host = Configuration.configuration.HOST_SSLAUTH;
		SocketAddress socketAddress = host.getSocketAddress();
		boolean isSSL = host.isSsl();

		// first clean if ask
		if (clean) {
			// Update all UpdatedInfo to DONE
			// where GlobalLastStep = ALLDONETASK and status = CompleteOk
			try {
				DbTaskRunner.changeFinishedToDone(DbConstant.admin.session);
			} catch (WaarpDatabaseNoConnectionException e) {
				logger.warn("Clean cannot be done {}", e.getMessage());
			}
		}
		LocalChannelReference localChannelReference = networkTransaction
				.createConnectionWithRetry(socketAddress, isSSL, future);
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
		future.awaitUninterruptibly();
		logger.info("Request done with " + (future.isSuccess() ? "success" : "error"));
		Channels.close(localChannelReference.getLocalChannel());
		localChannelReference = null;
	}

	protected static boolean spurgeLog = false;
	protected static Timestamp sstart = null;
	protected static Timestamp sstop = null;
	protected static boolean sclean = false;

	private static SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");

	private static Timestamp fixDate(String sdate) {
		Timestamp tdate = null;
		String date = sdate.replaceAll("/|:|\\.| |-", "");
		if (date.length() > 0) {
			if (date.length() < 15) {
				int len = date.length();
				date += "000000000000000".substring(len);
			}
			try {
				Date ddate = format.parse(date);
				tdate = new Timestamp(ddate.getTime());
			} catch (ParseException e) {
				logger.debug("start {}", e.getMessage());
			}
		}
		return tdate;
	}

	private static Timestamp fixDate(String sdate, Timestamp before) {
		Timestamp tdate = null;
		String date = sdate.replaceAll("/|:|\\.| |-", "");
		if (date.length() > 0) {
			if (date.length() < 15) {
				int len = date.length();
				date += "000000000000000".substring(len);
			}
			try {
				Date ddate = format.parse(date);
				if (before != null) {
					Date bef = new Date(before.getTime());
					if (bef.compareTo(ddate) >= 0) {
						ddate = new Date(bef.getTime() + 1000 * 3600 * 24 - 1);
					}
				}
				tdate = new Timestamp(ddate.getTime());
			} catch (ParseException e) {
				logger.debug("start {}", e.getMessage());
			}
		}
		return tdate;
	}

	private static Timestamp getTodayMidnight() {
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.set(GregorianCalendar.HOUR_OF_DAY, 0);
		calendar.set(GregorianCalendar.MINUTE, 0);
		calendar.set(GregorianCalendar.SECOND, 0);
		calendar.set(GregorianCalendar.MILLISECOND, 0);
		return new Timestamp(calendar.getTimeInMillis());
	}

	protected static boolean getParams(String[] args) {
		if (args.length < 1) {
			logger.error("Need at least the configuration file as first argument then optionally\n"
					+
					"    -purge\n"
					+
					"    -clean\n"
					+
					"    -start timestamp in format yyyyMMddHHmmssSSS possibly truncated and where one of ':-. ' can be separators\n"
					+
					"    -stop timestamp in same format than start\n" +
					"If not start and no stop are given, stop is Today Midnight (00:00:00)\n" +
					"If start is equals or greater than stop, stop is start+24H");
			return false;
		}
		if (!FileBasedConfiguration
				.setClientConfigurationFromXml(Configuration.configuration, args[0])) {
			logger.error("Need at least the configuration file as first argument then optionally\n"
					+
					"    -purge\n"
					+
					"    -clean\n"
					+
					"    -start timestamp in format yyyyMMddHHmmssSSS possibly truncated and where one of ':-. ' can be separators\n"
					+
					"    -stop timestamp in same format than start\n" +
					"If not start and no stop are given, stop is Today Midnight (00:00:00)\n" +
					"If start is equals or greater than stop, stop is start+24H");
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
			}
		}
		if (ssstart != null) {
			Timestamp tstart = fixDate(ssstart);
			if (tstart != null) {
				sstart = tstart;
			}
		}
		if (ssstop != null) {
			Timestamp tstop = fixDate(ssstop, sstart);
			if (tstop != null) {
				sstop = tstop;
			}
		}
		if (ssstart == null && ssstop == null) {
			sstop = getTodayMidnight();
		}
		return true;
	}

	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(LogExport.class);
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
			LogExport transaction = new LogExport(future,
					spurgeLog, sclean, sstart, sstop,
					networkTransaction);
			transaction.run();
			future.awaitUninterruptibly();
			long time2 = System.currentTimeMillis();
			long delay = time2 - time1;
			R66Result result = future.getResult();
			if (future.isSuccess()) {
				if (result.code == ErrorCode.Warning) {
					logger.warn("WARNED on file:\n    " +
							(result.other != null ? ((ValidPacket) result.other).getSheader() :
									"no file")
							+ "\n    delay: " + delay);
				} else {
					logger.warn("SUCCESS on Final file:\n    " +
							(result.other != null ? ((ValidPacket) result.other).getSheader() :
									"no file")
							+ "\n    delay: " + delay);
				}
			} else {
				if (result.code == ErrorCode.Warning) {
					logger.warn("Transfer is\n    WARNED", future.getCause());
					networkTransaction.closeAll();
					System.exit(result.code.ordinal());
				} else {
					logger.error("Transfer in\n    FAILURE", future.getCause());
					networkTransaction.closeAll();
					System.exit(result.code.ordinal());
				}
			}
		} finally {
			networkTransaction.closeAll();
		}
	}

}
