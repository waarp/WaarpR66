/**
 * Copyright 2009, Frederic Bregier, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package openr66.commander;

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.netty.channel.Channels;

import openr66.client.RecvThroughHandler;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.R66Session;
import openr66.context.authentication.R66Auth;
import openr66.context.filesystem.R66Dir;
import openr66.context.filesystem.R66File;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.DbConstant;
import openr66.database.data.AbstractDbData;
import openr66.database.data.DbHostAuth;
import openr66.database.data.DbTaskRunner;
import openr66.database.data.AbstractDbData.UpdatedInfo;
import openr66.database.data.DbTaskRunner.TASKSTEP;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolNotYetConnectionException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.ChannelUtils;
import openr66.protocol.utils.R66Future;

/**
 * Client Runner from a TaskRunner
 *
 * @author Frederic Bregier
 *
 */
public class ClientRunner implements Runnable {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(ClientRunner.class);

    private static final ConcurrentHashMap<String, Integer> taskRunnerRetryHashMap
         = new ConcurrentHashMap<String, Integer>();

    private final NetworkTransaction networkTransaction;
    private final DbTaskRunner taskRunner;
    private final R66Future futureRequest;
    private RecvThroughHandler handler = null;

    public ClientRunner(NetworkTransaction networkTransaction, DbTaskRunner taskRunner,
            R66Future futureRequest) {
        this.networkTransaction = networkTransaction;
        this.taskRunner = taskRunner;
        this.futureRequest = futureRequest;
    }

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        R66Future transfer;
        try {
            transfer = this.runTransfer();
        } catch (OpenR66RunnerErrorException e) {
            logger.error("Runner Error", e);
            return;
        } catch (OpenR66ProtocolNoConnectionException e) {
            logger.error("No connection Error {}", e.getMessage());
            return;
        } catch (OpenR66ProtocolPacketException e) {
            logger.error("Protocol Error", e);
            return;
        } catch (OpenR66ProtocolNotYetConnectionException e) {
            logger.info("No connection warning {}", e.getMessage());
            return;
        }
        R66Result result = transfer.getResult();
        if (result != null) {
            if (result.code == ErrorCode.QueryAlreadyFinished) {
                logger.info("TRANSFER RESULT:\n    "+(transfer.isSuccess()?"SUCCESS":"FAILURE")+"\n    "+
                        ErrorCode.QueryAlreadyFinished.mesg+":"+
                        (result != null ? result.toString() : "no result"));
            } else {
                logger.warn("TRANSFER RESULT:\n    "+(transfer.isSuccess()?"SUCCESS":"FAILURE")+"\n    "+
                        (result != null ? result.toString() : "no result"));
            }
        } else {
            logger.warn("TRANSFER RESULT:\n    "+(transfer.isSuccess()?"SUCCESS":"FAILURE")+"\n    "+
                "no result");
        }
        transfer = null;
    }
    /**
     *
     * @param runner
     * @param limit
     * @return True if the task was run less than limit, else False
     */
    private boolean incrementTaskRunerTry(DbTaskRunner runner, int limit) {
        String key = runner.getKey();
        Integer tries = taskRunnerRetryHashMap.get(key);
        if (tries == null) {
            tries = new Integer(1);
        } else {
            tries = tries+1;
        }
        if (limit <= tries) {
            taskRunnerRetryHashMap.remove(key);
            return false;
        } else {
            taskRunnerRetryHashMap.put(key, tries);
            return true;
        }
    }

    /**
     * True transfer run (can be called directly to enable exception outside any executors)
     * @return The R66Future of the transfer operation
     * @throws OpenR66RunnerErrorException
     * @throws OpenR66ProtocolNoConnectionException
     * @throws OpenR66ProtocolPacketException
     * @throws OpenR66ProtocolNotYetConnectionException
     */
    public R66Future runTransfer()
    throws OpenR66RunnerErrorException, OpenR66ProtocolNoConnectionException, OpenR66ProtocolPacketException, OpenR66ProtocolNotYetConnectionException {
        LocalChannelReference localChannelReference = initRequest();
        R66Future transfer = localChannelReference.getFutureRequest();
        try {
            transfer.await();
        } catch (InterruptedException e1) {
        }
        logger.info("Request done with {}",(transfer.isSuccess()?"success":"error"));

        Channels.close(localChannelReference.getLocalChannel());
        // now reload TaskRunner if it still exists (light client can forget it)
        if (transfer.isSuccess()) {
            try {
                taskRunner.select();
                this.changeUpdatedInfo(UpdatedInfo.DONE, ErrorCode.CompleteOk);
            } catch (OpenR66DatabaseException e) {
                logger.info("Not a problem but cannot find at the end the task");
            }
        } else {
            try {
                taskRunner.select();
                if (transfer.getResult().code == ErrorCode.QueryAlreadyFinished) {
                    // check if post task to execute
                    logger.warn("WARN QueryAlreadyFinished:\n    "+transfer.toString()+"\n    "+
                            taskRunner.toShortString());
                    finalizeLocalTask(taskRunner, localChannelReference);
                } else {
                    switch (taskRunner.getUpdatedInfo()) {
                        case DONE:
                        case INERROR:
                        case INTERRUPTED:
                            break;
                        default:
                            this.changeUpdatedInfo(UpdatedInfo.INERROR, transfer.getResult().code);
                    }
                }
            } catch (OpenR66DatabaseException e) {
                logger.info("Not a problem but cannot find at the end the task");
            }
        }
        return transfer;
    }
    /**
     * Finalize a local task since only Post action has to be done
     * @param taskRunner
     * @param localChannelReference
     * @throws OpenR66RunnerErrorException
     */
    public static void finalizeLocalTask(DbTaskRunner taskRunner,
            LocalChannelReference localChannelReference)
    throws OpenR66RunnerErrorException {
        R66Session session = new R66Session();
        session.setStatus(50);
        session.getAuth().specialHttpAuth(false);
        R66File file;
        try {
            file = new R66File(session,
                    new R66Dir(session), taskRunner.getFilename(), false);
        } catch (CommandAbstractException e) {
            logger.warn("Cannot recreate file: {}",taskRunner.getFilename());
            taskRunner.changeUpdatedInfo(UpdatedInfo.INERROR);
            taskRunner.setErrorExecutionStatus(ErrorCode.Internal);
            try {
                taskRunner.update();
            } catch (OpenR66DatabaseException e1) {
            }
            throw new OpenR66RunnerErrorException("Cannot recreate file", e);
        }
        R66Result finalValue = new R66Result(null, true, ErrorCode.CompleteOk, taskRunner);
        finalValue.file = file;
        finalValue.runner = taskRunner;
        try {
            taskRunner.finalizeTransfer(localChannelReference, file, finalValue, true);
        } catch (OpenR66ProtocolSystemException e) {
            logger.warn("Cannot validate runner:\n    {}",taskRunner.toShortString());
            taskRunner.changeUpdatedInfo(UpdatedInfo.INERROR);
            taskRunner.setErrorExecutionStatus(ErrorCode.Internal);
            try {
                taskRunner.update();
            } catch (OpenR66DatabaseException e1) {
            }
            throw new OpenR66RunnerErrorException("Cannot validate runner", e);
        }
    }
    /**
     * Initialize the request
     * @return the localChannelReference holding the transfer request
     * @throws OpenR66ProtocolNoConnectionException
     * @throws OpenR66RunnerErrorException
     * @throws OpenR66ProtocolPacketException
     * @throws OpenR66ProtocolNotYetConnectionException
     */
    public LocalChannelReference initRequest()
        throws OpenR66ProtocolNoConnectionException, OpenR66RunnerErrorException, OpenR66ProtocolPacketException, OpenR66ProtocolNotYetConnectionException {
        this.changeUpdatedInfo(UpdatedInfo.RUNNING, ErrorCode.Running);
        long id = taskRunner.getSpecialId();
        String tid;
        if (id == DbConstant.ILLEGALVALUE) {
            tid = taskRunner.getRuleId()+"_"+taskRunner.getMode()+"_NEWTRANSFER";
        } else {
            tid = taskRunner.getRuleId()+"_"+taskRunner.getMode()+"_"+id;
        }
        Thread.currentThread().setName(tid);
        logger.info("Will run {}",this.taskRunner);

        if (taskRunner.isSelfRequested()) {
            if (taskRunner.getGloballaststep() == TASKSTEP.POSTTASK.ordinal()) {
                // can finalize locally
                LocalChannelReference localChannelReference =
                    new LocalChannelReference();
                finalizeLocalTask(taskRunner, localChannelReference);
                logger.warn("Finalize as Restart:\n    "+taskRunner.toShortString());
                throw new OpenR66ProtocolNoConnectionException("Finalize as restart");
            }
            // Don't have to restart a task for itself (or should use requester)
            logger.warn("Requested host cannot initiate itself the request");
            this.changeUpdatedInfo(UpdatedInfo.INERROR, ErrorCode.NotKnownHost);
            throw new OpenR66RunnerErrorException("Requested host cannot initiate itself the request");
        }
        DbHostAuth host = R66Auth.getServerAuth(DbConstant.admin.session,
                taskRunner.getRequested());
        if (host == null) {
            logger.warn("Requested host cannot be found: "+taskRunner.getRequested());
            //taskRunner.setExecutionStatus(ErrorCode.NotKnownHost);
            this.changeUpdatedInfo(UpdatedInfo.INERROR, ErrorCode.NotKnownHost);
            throw new OpenR66RunnerErrorException("Requested host cannot be found "+
                    taskRunner.getRequested());
        }
        SocketAddress socketAddress = host.getSocketAddress();
        boolean isSSL = host.isSsl();

        LocalChannelReference localChannelReference = networkTransaction
            .createConnectionWithRetry(socketAddress, isSSL, futureRequest);
        socketAddress = null;
        if (localChannelReference == null) {
            // propose to redo
            // See if reprogramming is ok (not too many tries)
            String retry;
            if (incrementTaskRunerTry(taskRunner, Configuration.RETRYNB)) {
                logger.info("Will retry since Cannot connect to {}", host);
                retry = " but will retry";
                // now wait
                try {
                    Thread.sleep(Configuration.configuration.delayRetry);
                } catch (InterruptedException e) {
                }
                this.changeUpdatedInfo(UpdatedInfo.TOSUBMIT, ErrorCode.ConnectionImpossible);
                throw new OpenR66ProtocolNotYetConnectionException("Cannot connect to server "+
                        host.toString()+retry);
            } else {
                logger.info("Will not retry since limit of connection attemtps is reached for {}",
                        host);
                retry = " and retries limit is reached so stop here";
                this.changeUpdatedInfo(UpdatedInfo.INERROR, ErrorCode.ConnectionImpossible);
                throw new OpenR66ProtocolNoConnectionException("Cannot connect to server "+
                        host.toString()+retry);
            }
        }
        if (handler != null) {
            localChannelReference.setRecvThroughHandler(handler);
        }
        RequestPacket request = taskRunner.getRequest();
        logger.debug("Will send request {} ",request);
        try {
            ChannelUtils.writeAbstractLocalPacket(localChannelReference, request);
        } catch (OpenR66ProtocolPacketException e) {
            // propose to redo
            logger.warn("Cannot transfer request to "+host.toString());
            this.changeUpdatedInfo(UpdatedInfo.INTERRUPTED, ErrorCode.Internal);
            Channels.close(localChannelReference.getLocalChannel());
            localChannelReference = null;
            host = null;
            request = null;
            throw e;
        }
        logger.debug("Wait for request to {}",host);
        request = null;
        host = null;
        return localChannelReference;
    }
    /**
     * Change the UpdatedInfo of the current runner
     * @param info
     */
    public void changeUpdatedInfo(AbstractDbData.UpdatedInfo info, ErrorCode code) {
        this.taskRunner.changeUpdatedInfo(info);
        this.taskRunner.setErrorExecutionStatus(code);
        try {
            this.taskRunner.update();
        } catch (OpenR66DatabaseException e) {
        }
    }

    /**
     * @param handler the handler to set
     */
    public void setRecvThroughHandler(RecvThroughHandler handler) {
        this.handler = handler;
    }

}
