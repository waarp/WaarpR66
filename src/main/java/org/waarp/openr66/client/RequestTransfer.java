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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.waarp.common.database.data.AbstractDbData.UpdatedInfo;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.configuration.FileBasedConfiguration;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66FiniteDualStates;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.authentication.R66Auth;
import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.PartnerConfiguration;
import org.waarp.openr66.protocol.exception.OpenR66DatabaseGlobalException;
import org.waarp.openr66.protocol.exception.OpenR66Exception;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;
import org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket;
import org.waarp.openr66.protocol.localhandler.packet.JsonCommandPacket;
import org.waarp.openr66.protocol.localhandler.packet.LocalPacketFactory;
import org.waarp.openr66.protocol.localhandler.packet.ValidPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.ValidJsonPacket;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;


/**
 * Class to request information or request cancellation or restart
 * 
 * @author Frederic Bregier
 * 
 */
public class RequestTransfer implements Runnable {
	/**
	 * Internal Logger
	 */
	static volatile WaarpInternalLogger logger;

	protected static String _INFO_ARGS = 
			"Needs at least 5 arguments:\n" +
					"  the XML client configuration file,\n" +
					"  '-id' the transfer Id,\n" +
					"  '-to' the requested host Id or '-from' the requester host Id " +
					"(localhost will be the opposite),\n" +
					"Other options (only one):\n" +
					"  '-cancel' to cancel completely the transfer,\n" +
					"  '-stop' to stop the transfer (maybe restarted),\n" +
					"  '-restart' to restart if possible a transfer and optionnally the following arguments may be specified for a restart:\n"+
					"      '-start' \"time start\" as yyyyMMddHHmmss (override previous -delay options)\n"
					+
					"      '-delay' \"+delay in ms\" as delay in ms from current time(override previous -start options)\n"
					+
					"      '-delay' \"delay in ms\" as time in ms (override previous -start options)";
	
	protected final NetworkTransaction networkTransaction;
	final R66Future future;
	final long specialId;
	String requested = null;
	String requester = null;
	boolean cancel = false;
	boolean stop = false;
	boolean restart = false;
	String restarttime = null;

	static long sspecialId;
	static String srequested = null;
	static String srequester = null;
	static boolean scancel = false;
	static boolean sstop = false;
	static boolean srestart = false;
	static String srestarttime = null;

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
			if (args[i].equalsIgnoreCase("-id")) {
				i++;
				try {
					sspecialId = Long.parseLong(args[i]);
				} catch (NumberFormatException e) {
					logger.error("Cannot value Id: "+args[i],e);
					return false;
				}
			} else if (args[i].equalsIgnoreCase("-to")) {
				i++;
				srequested = args[i];
				try {
					srequester = Configuration.configuration.getHostId(DbConstant.admin.session,
							srequested);
				} catch (WaarpDatabaseException e) {
					logger.error("Cannot get Host Id: " + srequester, e);
					return false;
				}
			} else if (args[i].equalsIgnoreCase("-from")) {
				i++;
				srequester = args[i];
				try {
					srequested = Configuration.configuration.getHostId(DbConstant.admin.session,
							srequester);
				} catch (WaarpDatabaseException e) {
					logger.error("Cannot get Host Id: " + srequested, e);
					return false;
				}
			} else if (args[i].equalsIgnoreCase("-cancel")) {
				scancel = true;
			} else if (args[i].equalsIgnoreCase("-stop")) {
				sstop = true;
			} else if (args[i].equalsIgnoreCase("-restart")) {
				srestart = true;
			} else if (args[i].equalsIgnoreCase("-start")) {
				i++;
				srestarttime = args[i];
			} else if (args[i].equalsIgnoreCase("-delay")) {
				i++;
				if (args[i].charAt(0) == '+') {
					Date date = new Date(System.currentTimeMillis() +
							Long.parseLong(args[i].substring(1)));
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
					srestarttime = dateFormat.format(date);
				} else {
					Date date = new Date(Long.parseLong(args[i]));
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
					srestarttime = dateFormat.format(date);
				}
			}
		}
		if ((scancel && srestart) || (scancel && sstop) || (srestart && sstop)) {
			logger.error("Cannot cancel or restart or stop at the same time\n"+_INFO_ARGS);
			return false;
		}
		if (sspecialId == DbConstant.ILLEGALVALUE || srequested == null) {
			logger.error("TransferId and Requested/Requester HostId must be set\n"+_INFO_ARGS);
			return false;
		}

		return true;
	}

	/**
	 * @param future
	 * @param specialId
	 * @param requested
	 * @param requester
	 * @param cancel
	 * @param stop
	 * @param restart
	 * @param networkTransaction
	 */
	public RequestTransfer(R66Future future, long specialId, String requested, String requester,
			boolean cancel, boolean stop, boolean restart,
			NetworkTransaction networkTransaction) {
		this(future, specialId, requested, requester, cancel, stop, restart, null, networkTransaction);
	}

	/**
	 * @param future
	 * @param specialId
	 * @param requested
	 * @param requester
	 * @param cancel
	 * @param stop
	 * @param restart
	 * @param restarttime in yyyyMMddHHmmss format
	 * @param networkTransaction
	 */
	public RequestTransfer(R66Future future, long specialId, String requested, String requester,
			boolean cancel, boolean stop, boolean restart, String restarttime,
			NetworkTransaction networkTransaction) {
		this.future = future;
		this.specialId = specialId;
		this.requested = requested;
		this.requester = requester;
		this.cancel = cancel;
		this.stop = stop;
		this.restart = restart;
		this.restarttime = restarttime;
		this.networkTransaction = networkTransaction;
	}
	
	public void run() {
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(RequestTransfer.class);
		}
		DbTaskRunner runner = null;
		try {
			runner = new DbTaskRunner(DbConstant.admin.session, null, null,
					specialId, requester, requested);
			logger.info("Found previous Runner: "+runner.toString());
		} catch (WaarpDatabaseException e) {
			// Maybe we can ask to the remote
			R66Future futureInfo = new R66Future(true);
			RequestInformation requestInformation = new RequestInformation(futureInfo, srequested, null, null, (byte) -1, specialId, true, networkTransaction);
			requestInformation.run();
			futureInfo.awaitUninterruptibly();
			if (futureInfo.isSuccess()) {
				R66Result r66result = futureInfo.getResult();
				ValidPacket info = (ValidPacket) r66result.other;
				String xml = info.getSheader();
				Document document;
				try {
					document = DocumentHelper.parseText(xml);
				} catch (DocumentException e1) {
					logger.error("Cannot find the transfer");
					future.setResult(new R66Result(new OpenR66DatabaseGlobalException(e1), null, true,
							ErrorCode.Internal, null));
					future.setFailure(e);
					return;
				}
				try {
					runner = DbTaskRunner.fromXml(DbConstant.admin.session, document.getRootElement(), true);
					logger.info("Get Runner from remote: "+runner.toString());
					if (runner.getSpecialId() == DbConstant.ILLEGALVALUE || ! runner.isSender()) {
						logger.error("Cannot find the transfer");
						future.setResult(new R66Result(new OpenR66DatabaseGlobalException(e), null, true,
								ErrorCode.Internal, null));
						future.setFailure(e);
						return;
					}
					if (runner.isAllDone()) {
						logger.error("Transfer already finished");
						future.setResult(new R66Result(new OpenR66DatabaseGlobalException(e), null, true,
								ErrorCode.Internal, null));
						future.setFailure(e);
						return;
					}
				} catch (OpenR66ProtocolBusinessException e1) {
					logger.error("Cannot find the transfer");
					future.setResult(new R66Result(new OpenR66DatabaseGlobalException(e1), null, true,
							ErrorCode.Internal, null));
					future.setFailure(e);
					return;
				}
			} else {
				logger.error("Cannot find the transfer");
				future.setResult(new R66Result(new OpenR66DatabaseGlobalException(e), null, true,
						ErrorCode.Internal, null));
				future.setFailure(e);
				return;
			}
		}
		if (cancel || stop || restart) {
			if (cancel) {
				// Cancel the task and delete any file if in retrieve
				if (runner.isAllDone()) {
					// nothing to do since already finished
					setDone(runner);
					logger.info("Transfer already finished: " + runner.toString());
					future.setResult(new R66Result(null, true, ErrorCode.TransferOk, runner));
					future.getResult().runner = runner;
					future.setSuccess();
					return;
				} else {
					// Send a request of cancel
					ErrorCode code = sendValid(runner, LocalPacketFactory.CANCELPACKET);
					switch (code) {
						case CompleteOk:
							logger.info("Transfer cancel requested and done: {}",
									runner);
							break;
						case TransferOk:
							logger.info("Transfer cancel requested but already finished: {}",
									runner);
							break;
						default:
							logger.info("Transfer cancel requested but internal error: {}",
									runner);
							break;
					}
				}
			} else if (stop) {
				// Just stop the task
				// Send a request
				ErrorCode code = sendValid(runner, LocalPacketFactory.STOPPACKET);
				switch (code) {
					case CompleteOk:
						logger.info("Transfer stop requested and done: {}", runner);
						break;
					case TransferOk:
						logger.info("Transfer stop requested but already finished: {}",
								runner);
						break;
					default:
						logger.info("Transfer stop requested but internal error: {}",
								runner);
						break;
				}
			} else if (restart) {
				// Restart if already stopped and not finished
				ErrorCode code = sendValid(runner, LocalPacketFactory.VALIDPACKET);
				switch (code) {
					case QueryStillRunning:
						logger.info(
								"Transfer restart requested but already active and running: {}",
								runner);
						break;
					case Running:
						logger.info("Transfer restart requested but already running: {}",
								runner);
						break;
					case PreProcessingOk:
						logger.info("Transfer restart requested and restarted: {}",
								runner);
						break;
					case CompleteOk:
						logger.info("Transfer restart requested but already finished: {}",
								runner);
						break;
					case RemoteError:
						logger.info("Transfer restart requested but remote error: {}",
								runner);
						break;
					case PassThroughMode:
						logger.info("Transfer not restarted since it is in PassThrough mode: {}",
								runner);
						break;
					default:
						logger.info("Transfer restart requested but internal error: {}",
								runner);
						break;
				}
			}
		} else {
			// Only request
			logger.info("Transfer information:     " + runner.toShortString());
			future.setResult(new R66Result(null, true, runner.getErrorInfo(), runner));
			future.setSuccess();
		}
	}

	/**
	 * Set the runner to DONE
	 * 
	 * @param runner
	 */
	private void setDone(DbTaskRunner runner) {
		if (runner.getUpdatedInfo() != UpdatedInfo.DONE) {
			runner.changeUpdatedInfo(UpdatedInfo.DONE);
			runner.forceSaveStatus();
		}
	}

	private ErrorCode sendValid(DbTaskRunner runner, byte code) {
		DbHostAuth host;
		host = R66Auth.getServerAuth(DbConstant.admin.session,
				this.requester);
		if (host == null) {
			logger.error("Requester host cannot be found: " + this.requester);
			OpenR66Exception e =
					new OpenR66RunnerErrorException("Requester host cannot be found");
			future.setResult(new R66Result(
					e,
					null, true,
					ErrorCode.TransferError, null));
			future.setFailure(e);
			return ErrorCode.Internal;
		}
		// check if requester is "client" so no connect from him but direct action
		logger.debug("Requester Host isClient: "+host.isClient());
		if (host.isClient()) {
			if (code == LocalPacketFactory.VALIDPACKET) {
				logger.warn("RequestTransfer from Client as requester, so use DirectTransfer instead: "+
						runner.toShortString());
				R66Future transfer = new R66Future(true);
				DirectTransfer transaction = new DirectTransfer(transfer,
						runner.getRequested(), runner.getOriginalFilename(), 
						runner.getRuleId(), runner.getFileInformation(), false, 
						runner.getBlocksize(), runner.getSpecialId(), networkTransaction);
				transaction.run();
				transfer.awaitUninterruptibly();
				logger.info("Request done with " + (transfer.isSuccess() ? "success" : "error"));
				if (transfer.isSuccess()) {
					future.setResult(new R66Result(null, true, ErrorCode.PreProcessingOk, runner));
					future.getResult().runner = runner;
					future.setSuccess();
					return ErrorCode.PreProcessingOk;
				} else {
					R66Result result = transfer.getResult();
					ErrorCode error = ErrorCode.Internal;
					if (result != null) {
						error = result.code;
					}
					OpenR66Exception e =
							new OpenR66RunnerErrorException("Transfer in direct mode failed: "+error.mesg);
					future.setFailure(e);
					return error;
				}
			} else {
				// get remote host instead
				host = R66Auth.getServerAuth(DbConstant.admin.session,
						this.requested);
				if (host == null) {
					logger.error("Requested host cannot be found: " + this.requested);
					OpenR66Exception e =
							new OpenR66RunnerErrorException("Requested host cannot be found");
					future.setResult(new R66Result(
							e,
							null, true,
							ErrorCode.TransferError, null));
					future.setFailure(e);
					return ErrorCode.Internal;
				}
			}
		}

		logger.info("Try RequestTransfer to "+host.toString());
		SocketAddress socketAddress = host.getSocketAddress();
		boolean isSSL = host.isSsl();

		LocalChannelReference localChannelReference = networkTransaction
				.createConnectionWithRetry(socketAddress, isSSL, future);
		socketAddress = null;
		if (localChannelReference == null) {
			logger.debug("Cannot connect to " + host.toString());
			host = null;
			future.setResult(new R66Result(null, true,
					ErrorCode.ConnectionImpossible, null));
			future.cancel();
			return ErrorCode.Internal;
		}
		boolean useJson = PartnerConfiguration.useJson(host.getHostid());
		logger.debug("UseJson: "+useJson);
		AbstractLocalPacket packet = null;
		if (useJson) {
			ValidJsonPacket node = new ValidJsonPacket();
			node.setComment("Request on Transfer");
			node.setRequested(requested);
			node.setRequester(requester);
			node.setSpecialid(specialId);
			if (restarttime != null && code == LocalPacketFactory.VALIDPACKET) {
				// restart time set
				logger.debug("Restart with time: "+restarttime);
				// time to reschedule in yyyyMMddHHmmss format
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
				try {
					Date date = dateFormat.parse(restarttime);
					node.setRestarttime(date);
				} catch (ParseException e) {
				}
				packet = new JsonCommandPacket(node, code);
			} else {
				packet = new JsonCommandPacket(node, code);
			}
		} else {
			if (restarttime != null && code == LocalPacketFactory.VALIDPACKET) {
				// restart time set
				logger.debug("Restart with time: "+restarttime);
				packet = new ValidPacket("Request on Transfer",
						this.requested + " " + this.requester + " " + this.specialId+" "+restarttime,
						code);
			} else {
				packet = new ValidPacket("Request on Transfer",
					this.requested + " " + this.requester + " " + this.specialId,
					code);
			}
		}
		localChannelReference.sessionNewState(R66FiniteDualStates.VALIDOTHER);
		try {
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, packet, false);
		} catch (OpenR66ProtocolPacketException e) {
			logger.error("Cannot transfer request to " + host.toString());
			Channels.close(localChannelReference.getLocalChannel());
			localChannelReference = null;
			host = null;
			packet = null;
			logger.debug("Bad Protocol", e);
			future.setResult(new R66Result(e, null, true,
					ErrorCode.TransferError, null));
			future.setFailure(e);
			return ErrorCode.Internal;
		}
		packet = null;
		host = null;
		future.awaitUninterruptibly();

		Channels.close(localChannelReference.getLocalChannel());
		localChannelReference = null;

		logger.info("Request done with " + (future.isSuccess() ? "success" : "error"));
		R66Result result = future.getResult();
		if (result != null) {
			return result.code;
		}
		return ErrorCode.Internal;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(RequestTransfer.class);
		}
		if (!getParams(args)) {
			logger.error("Wrong initialization");
			if (DbConstant.admin != null && DbConstant.admin.isConnected) {
				DbConstant.admin.close();
			}
			ChannelUtils.stopLogger();
			System.exit(1);
		}
		int value = 99;
		try {
			Configuration.configuration.pipelineInit();
			NetworkTransaction networkTransaction = new NetworkTransaction();
			R66Future result = new R66Future(true);
			RequestTransfer requestTransfer =
					new RequestTransfer(result, sspecialId, srequested, srequester,
							scancel, sstop, srestart, srestarttime,
							networkTransaction);
			requestTransfer.run();
			result.awaitUninterruptibly();
			R66Result finalValue = result.getResult();
			if (scancel || sstop || srestart) {
				if (scancel) {
					if (result.isSuccess()) {
						value = 0;
						logger.warn("Transfer already finished:     " +
								finalValue.runner.toShortString());
					} else {
						switch (finalValue.code) {
							case CompleteOk:
								value = 0;
								logger.warn("Transfer cancel requested and done:     " +
										finalValue.runner.toShortString());
								break;
							case TransferOk:
								value = 3;
								logger.warn("Transfer cancel requested but already finished:     "
										+
										finalValue.runner.toShortString());
								break;
							default:
								value = 4;
								logger.error("Transfer cancel requested but internal error:     " +
										finalValue.runner.toShortString());
								break;
						}
					}
				} else if (sstop) {
					switch (finalValue.code) {
						case CompleteOk:
							value = 0;
							logger.warn("Transfer stop requested and done:     " +
									finalValue.runner.toShortString());
							break;
						case TransferOk:
							value = 0;
							logger.warn("Transfer stop requested but already finished:     " +
									finalValue.runner.toShortString());
							break;
						default:
							value = 3;
							logger.error("Transfer stop requested but internal error:     " +
									finalValue.runner.toShortString());
							break;
					}
				} else if (srestart) {
					switch (finalValue.code) {
						case QueryStillRunning:
							value = 0;
							logger.warn("Transfer restart requested but already active and running:     "
									+
									finalValue.runner.toShortString());
							break;
						case Running:
							value = 0;
							logger.warn("Transfer restart requested but already running:     " +
									finalValue.runner.toShortString());
							break;
						case PreProcessingOk:
							value = 0;
							logger.warn("Transfer restart requested and restarted:     " +
									finalValue.runner.toShortString());
							break;
						case CompleteOk:
							value = 4;
							logger.warn("Transfer restart requested but already finished:     " +
									finalValue.runner.toShortString());
							break;
						case RemoteError:
							value = 5;
							logger.error("Transfer restart requested but remote error:     " +
									finalValue.runner.toShortString());
							break;
						case PassThroughMode:
							value = 6;
							logger.warn("Transfer not restarted since it is in PassThrough mode:     "
									+
									finalValue.runner.toShortString());
							break;
						default:
							value = 3;
							logger.error("Transfer restart requested but internal error:     " +
									finalValue.runner.toShortString());
							break;
					}
				}
			} else {
				value = 0;
				// Only request
				logger.warn("Transfer information:     " +
						finalValue.runner.toShortString());
			}
		} finally {
			if (DbConstant.admin != null) {
				DbConstant.admin.close();
			}
			System.exit(value);
		}
	}

}
