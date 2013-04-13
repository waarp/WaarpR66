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
package org.waarp.openr66.protocol.utils;

import java.sql.Timestamp;

import org.waarp.common.command.exception.CommandAbstractException;
import org.waarp.common.database.DbPreparedStatement;
import org.waarp.common.database.DbSession;
import org.waarp.common.database.data.AbstractDbData.UpdatedInfo;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.openr66.client.RequestTransfer;
import org.waarp.openr66.commander.ClientRunner;
import org.waarp.openr66.commander.CommanderNoDb;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66FiniteDualStates;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.context.filesystem.R66File;
import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.database.data.DbTaskRunner.TASKSTEP;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;
import org.waarp.openr66.protocol.localhandler.packet.ErrorPacket;

/**
 * Utility class for transfers
 * 
 * @author Frederic Bregier
 * 
 */
public class TransferUtils {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(TransferUtils.class);

	/**
	 * Try to restart one Transfer Runner Task
	 * 
	 * @param taskRunner
	 * @return the associated Result
	 * @throws WaarpDatabaseException
	 */
	@SuppressWarnings("unused")
	public static R66Result restartTransfer(DbTaskRunner taskRunner, LocalChannelReference lcr)
			throws WaarpDatabaseException {
		R66Result finalResult = new R66Result(null, true, ErrorCode.InitOk, taskRunner);
		if (lcr != null) {
			finalResult.code = ErrorCode.QueryStillRunning;
			finalResult.other = "Transfer is still running so not restartable";
		} else {
			if (taskRunner.isSendThrough()) {
				// XXX FIXME TODO cannot be restarted... Really?
				if (false) {
					finalResult.code = ErrorCode.PassThroughMode;
					finalResult.other = "Transfer cannot be restarted since it is in PassThrough mode";
					return finalResult;
				}
			}
			// Transfer is not running
			// but maybe need action on database
			try {
				if (taskRunner.restart(true)) {
					taskRunner.forceSaveStatus();
					finalResult.code = ErrorCode.PreProcessingOk;
					finalResult.other = "Transfer is restarted";
				} else {
					if (taskRunner.isSelfRequested() &&
							(taskRunner.getGloballaststep() < TASKSTEP.POSTTASK.ordinal())) {
						// send a VALID packet with VALID code to the requester
						R66Future result = new R66Future(true);
						RequestTransfer requestTransfer =
								new RequestTransfer(result, taskRunner.getSpecialId(),
										taskRunner.getRequested(), taskRunner.getRequester(),
										false, false, true,
										Configuration.configuration.getInternalRunner().
												getNetworkTransaction());
						requestTransfer.run();
						result.awaitUninterruptibly();
						R66Result finalValue = result.getResult();
						switch (finalValue.code) {
							case QueryStillRunning:
								finalResult.code = ErrorCode.QueryStillRunning;
								finalResult.other = "Transfer restart requested but already active and running";
								break;
							case Running:
								finalResult.code = ErrorCode.Running;
								finalResult.other = "Transfer restart requested but already running";
								break;
							case PreProcessingOk:
								finalResult.code = ErrorCode.PreProcessingOk;
								finalResult.other = "Transfer restart requested and restarted";
								break;
							case CompleteOk:
								finalResult.code = ErrorCode.CompleteOk;
								finalResult.other = "Transfer restart requested but already finished so try to run Post Action";
								taskRunner.setPostTask();
								TransferUtils.finalizeTaskWithNoSession(taskRunner, lcr);
								taskRunner.setErrorExecutionStatus(ErrorCode.QueryAlreadyFinished);
								taskRunner.forceSaveStatus();
								break;
							case RemoteError:
								finalResult.code = ErrorCode.RemoteError;
								finalResult.other = "Transfer restart requested but remote error";
								break;
							default:
								finalResult.code = ErrorCode.Internal;
								finalResult.other = "Transfer restart requested but internal error";
								break;
						}
					} else {
						finalResult.code = ErrorCode.CompleteOk;
						finalResult.other = "Transfer is finished so not restartable";
						taskRunner.setPostTask();
						TransferUtils.finalizeTaskWithNoSession(taskRunner, lcr);
						taskRunner.setErrorExecutionStatus(ErrorCode.QueryAlreadyFinished);
						taskRunner.forceSaveStatus();
					}
				}
			} catch (OpenR66RunnerErrorException e) {
				finalResult.code = ErrorCode.PreProcessingOk;
				finalResult.other = "Transfer is restarted";
			}
		}
		return finalResult;
	}

	private static void stopOneTransfer(DbTaskRunner taskRunner,
			StringBuilder builder, R66Session session, String body) {
		LocalChannelReference lcr =
				Configuration.configuration.getLocalTransaction().
						getFromRequest(taskRunner.getKey());
		ErrorCode result;
		ErrorCode code = ErrorCode.StoppedTransfer;
		if (lcr != null) {
			int rank = taskRunner.getRank();
			lcr.sessionNewState(R66FiniteDualStates.ERROR);
			ErrorPacket perror = new ErrorPacket("Transfer Stopped at " + rank,
					code.getCode(), ErrorPacket.FORWARDCLOSECODE);
			try {
				// XXX ChannelUtils.writeAbstractLocalPacket(lcr, perror);
				// inform local instead of remote
				ChannelUtils.writeAbstractLocalPacketToLocal(lcr, perror);
			} catch (Exception e) {
			}
			result = ErrorCode.StoppedTransfer;
		} else {
			// Transfer is not running
			// if in ERROR already just ignore it
			if (taskRunner.getUpdatedInfo() == UpdatedInfo.INERROR) {
				result = ErrorCode.TransferError;
			} else {
				// the database saying it is not stopped
				result = ErrorCode.TransferError;
				if (taskRunner != null) {
					if (taskRunner.stopOrCancelRunner(code)) {
						result = ErrorCode.StoppedTransfer;
					}
				}
			}
		}
		ErrorCode last = taskRunner.getErrorInfo();
		taskRunner.setErrorExecutionStatus(result);
		if (builder != null) {
			builder.append(taskRunner.toSpecializedHtml(session, body,
					lcr != null ? "Active" : "NotActive"));
		}
		taskRunner.setErrorExecutionStatus(last);
	}

	/**
	 * Stop all selected transfers
	 * 
	 * @param dbSession
	 * @param limit
	 * @param builder
	 * @param session
	 * @param body
	 * @param startid
	 * @param stopid
	 * @param tstart
	 * @param tstop
	 * @param rule
	 * @param req
	 * @param pending
	 * @param transfer
	 * @param error
	 * @return the associated StringBuilder if the one given as parameter is not null
	 */
	public static StringBuilder stopSelectedTransfers(DbSession dbSession, int limit,
			StringBuilder builder, R66Session session, String body,
			String startid, String stopid, Timestamp tstart, Timestamp tstop, String rule,
			String req, boolean pending, boolean transfer, boolean error) {
		if (dbSession == null || dbSession.isDisconnected) {
			// do it without DB
			if (ClientRunner.activeRunners != null) {
				for (ClientRunner runner : ClientRunner.activeRunners) {
					DbTaskRunner taskRunner = runner.getTaskRunner();
					stopOneTransfer(taskRunner, builder, session, body);
				}
			}
			if (CommanderNoDb.todoList != null) {
				CommanderNoDb.todoList.clear();
			}
			return builder;
		}
		DbPreparedStatement preparedStatement = null;
		try {
			preparedStatement =
					DbTaskRunner.getFilterPrepareStatement(dbSession, limit, true,
							startid, stopid, tstart, tstop, rule, req,
							pending, transfer, error, false, false);
			preparedStatement.executeQuery();
			while (preparedStatement.getNext()) {
				DbTaskRunner taskRunner = DbTaskRunner.getFromStatement(preparedStatement);
				stopOneTransfer(taskRunner, builder, session, body);
			}
			preparedStatement.realClose();
			return builder;
		} catch (WaarpDatabaseException e) {
			if (preparedStatement != null) {
				preparedStatement.realClose();
			}
			logger.error("OpenR66 Error {}", e.getMessage());
			return null;
		}
	}

	/**
	 * Finalize a local task since only Post action has to be done
	 * 
	 * @param taskRunner
	 * @param localChannelReference
	 * @throws OpenR66RunnerErrorException
	 */
	public static void finalizeTaskWithNoSession(DbTaskRunner taskRunner,
			LocalChannelReference localChannelReference)
			throws OpenR66RunnerErrorException {
		R66Session session = new R66Session();
		session.setStatus(50);
		String remoteId = taskRunner.isSelfRequested() ?
				taskRunner.getRequester() :
				taskRunner.getRequested();
		session.getAuth().specialNoSessionAuth(false, remoteId);
		session.setNoSessionRunner(taskRunner, localChannelReference);
		if (taskRunner.isSender()) {
			// Change dir
			try {
				session.getDir().changeDirectory(taskRunner.getRule().sendPath);
			} catch (CommandAbstractException e) {
				throw new OpenR66RunnerErrorException(e);
			}
		} else {
			// Change dir
			try {
				session.getDir().changeDirectory(taskRunner.getRule().workPath);
			} catch (CommandAbstractException e) {
				throw new OpenR66RunnerErrorException(e);
			}
		}
		try {
			try {
				session.setFileAfterPreRunner(false);
			} catch (CommandAbstractException e) {
				throw new OpenR66RunnerErrorException(e);
			}
		} catch (OpenR66RunnerErrorException e) {
			logger.error("Cannot recreate file: {}", taskRunner.getFilename());
			taskRunner.changeUpdatedInfo(UpdatedInfo.INERROR);
			taskRunner.setErrorExecutionStatus(ErrorCode.FileNotFound);
			try {
				taskRunner.update();
			} catch (WaarpDatabaseException e1) {
			}
			throw new OpenR66RunnerErrorException("Cannot recreate file", e);
		}
		R66File file = session.getFile();
		R66Result finalValue = new R66Result(null, true, ErrorCode.CompleteOk, taskRunner);
		finalValue.file = file;
		finalValue.runner = taskRunner;
		taskRunner.finishTransferTask(ErrorCode.TransferOk);
		try {
			taskRunner.finalizeTransfer(localChannelReference, file, finalValue, true);
		} catch (OpenR66ProtocolSystemException e) {
			logger.error("Cannot validate runner:\n    {}", taskRunner.toShortString());
			taskRunner.changeUpdatedInfo(UpdatedInfo.INERROR);
			taskRunner.setErrorExecutionStatus(ErrorCode.Internal);
			try {
				taskRunner.update();
			} catch (WaarpDatabaseException e1) {
			}
			throw new OpenR66RunnerErrorException("Cannot validate runner", e);
		}
	}
}
