/**
   This file is part of GoldenGate Project (named also GoldenGate or GG).

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All GoldenGate Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   GoldenGate is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GoldenGate .  If not, see <http://www.gnu.org/licenses/>.
 */
package openr66.protocol.utils;

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.database.DbPreparedStatement;
import goldengate.common.database.DbSession;
import goldengate.common.database.data.AbstractDbData.UpdatedInfo;
import goldengate.common.database.exception.GoldenGateDatabaseException;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.sql.Timestamp;

import openr66.client.RequestTransfer;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.R66Session;
import openr66.context.filesystem.R66File;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.data.DbTaskRunner;
import openr66.database.data.DbTaskRunner.TASKSTEP;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.ErrorPacket;
import openr66.protocol.localhandler.packet.RequestPacket;

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
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(TransferUtils.class);

    /**
     * Try to restart one Transfer Runner Task
     * @param taskRunner
     * @return the associated Result
     * @throws GoldenGateDatabaseException
     */
    public static R66Result restartTransfer(DbTaskRunner taskRunner, LocalChannelReference lcr) throws GoldenGateDatabaseException {
        R66Result finalResult = new R66Result(null, true, ErrorCode.InitOk, taskRunner);
        if (lcr != null) {
            finalResult.code = ErrorCode.QueryStillRunning;
            finalResult.other = "Transfer is still running so not restartable";
        } else {
            if (RequestPacket.isRecvThroughMode(taskRunner.getMode()) ||
                    RequestPacket.isSendThroughMode(taskRunner.getMode())) {
                // cannot be restarted
                finalResult.code = ErrorCode.PassThroughMode;
                finalResult.other = "Transfer cannot be restarted since it is in PassThrough mode";
                return finalResult;
            }
            // Transfer is not running
            // but maybe need action on database
            try {
                if (taskRunner.restart(true)) {
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
                                taskRunner.update();
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
                        taskRunner.update();
                    }
                }
            } catch (OpenR66RunnerErrorException e) {
                finalResult.code = ErrorCode.PreProcessingOk;
                finalResult.other = "Transfer is restarted";
            }
        }
        return finalResult;
    }

    /**
     * Stop all selected transfers
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
        DbPreparedStatement preparedStatement = null;
        try {
            preparedStatement =
                DbTaskRunner.getFilterPrepareStatement(dbSession, limit, true,
                        startid, stopid, tstart, tstop, rule, req,
                        pending, transfer, error, false, false);
            preparedStatement.executeQuery();
            int i = 0;
            while (preparedStatement.getNext()) {
                i++;
                DbTaskRunner taskRunner = DbTaskRunner.getFromStatement(preparedStatement);
                LocalChannelReference lcr =
                    Configuration.configuration.getLocalTransaction().
                    getFromRequest(taskRunner.getKey());
                ErrorCode result;
                ErrorCode code = ErrorCode.StoppedTransfer;
                if (lcr != null) {
                    int rank = taskRunner.getRank();
                    ErrorPacket perror = new ErrorPacket("Transfer Stopped at "+rank,
                            code.getCode(), ErrorPacket.FORWARDCLOSECODE);
                    try {
                        //XXX ChannelUtils.writeAbstractLocalPacket(lcr, perror);
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
            preparedStatement.realClose();
            return builder;
        } catch (GoldenGateDatabaseException e) {
            if (preparedStatement != null) {
                preparedStatement.realClose();
            }
            logger.error("OpenR66 Error {}",e.getMessage());
            return null;
        }
    }

    /**
     * Finalize a local task since only Post action has to be done
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
            session.setFileAfterPreRunner(false);
        } catch (OpenR66RunnerErrorException e) {
            logger.error("Cannot recreate file: {}",taskRunner.getFilename());
            taskRunner.changeUpdatedInfo(UpdatedInfo.INERROR);
            taskRunner.setErrorExecutionStatus(ErrorCode.FileNotFound);
            try {
                taskRunner.update();
            } catch (GoldenGateDatabaseException e1) {
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
            logger.error("Cannot validate runner:\n    {}",taskRunner.toShortString());
            taskRunner.changeUpdatedInfo(UpdatedInfo.INERROR);
            taskRunner.setErrorExecutionStatus(ErrorCode.Internal);
            try {
                taskRunner.update();
            } catch (GoldenGateDatabaseException e1) {
            }
            throw new OpenR66RunnerErrorException("Cannot validate runner", e);
        }
    }
}
