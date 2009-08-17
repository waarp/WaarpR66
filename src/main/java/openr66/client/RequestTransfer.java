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
package openr66.client;

import java.net.SocketAddress;

import org.jboss.netty.channel.Channels;
import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;
import openr66.configuration.FileBasedConfiguration;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.authentication.R66Auth;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.DbConstant;
import openr66.database.data.DbHostAuth;
import openr66.database.data.DbTaskRunner;
import openr66.database.data.AbstractDbData.UpdatedInfo;
import openr66.database.data.DbTaskRunner.TASKSTEP;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
import openr66.protocol.localhandler.packet.ValidPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.ChannelUtils;
import openr66.protocol.utils.R66Future;

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
    static volatile GgInternalLogger logger;

    protected final NetworkTransaction networkTransaction;
    final R66Future future;
    final long specialId;
    String requested = null;
    String requester = null;
    boolean cancel = false;
    boolean stop = false;
    boolean restart = false;

    static long sspecialId;
    static String srequested = null;
    static String srequester = null;
    static boolean scancel = false;
    static boolean sstop = false;
    static boolean srestart = false;

    /**
     * Parse the parameter and set current values
     * @param args
     * @return True if all parameters were found and correct
     */
    protected static boolean getParams(String []args) {
        if (args.length < 3) {
            logger
                    .error("Needs at least the configuration file, the transfer id and " +
                    		"the requested hostId");
            return false;
        }
        if (! FileBasedConfiguration
                .setClientConfigurationFromXml(args[0])) {
            logger
                    .error("Needs a correct configuration file as first argument");
            return false;
        }
        for (int i = 1; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-id")) {
                i++;
                sspecialId = Long.parseLong(args[i]);
            } else if (args[i].equalsIgnoreCase("-to")) {
                i++;
                srequested = args[i];
                try {
                    srequester = Configuration.configuration.getHostId(DbConstant.admin.session,
                            srequested);
                } catch (OpenR66DatabaseException e) {
                    logger.error("Cannot get Host Id",e);
                    return false;
                }
            } else if (args[i].equalsIgnoreCase("-from")) {
                i++;
                srequester = args[i];
                try {
                    srequested = Configuration.configuration.getHostId(DbConstant.admin.session,
                            srequester);
                } catch (OpenR66DatabaseException e) {
                    logger.error("Cannot get Host Id",e);
                    return false;
                }
            } else if (args[i].equalsIgnoreCase("-cancel")) {
                scancel = true;
            } else if (args[i].equalsIgnoreCase("-stop")) {
                sstop = true;
            } else if (args[i].equalsIgnoreCase("-restart")) {
                srestart = true;
            }
        }
        if ((scancel && srestart) || (scancel && sstop) || (srestart && sstop)) {
            logger.error("Cannot cancel or restart or stop at the same time");
            return false;
        }
        if (sspecialId == DbConstant.ILLEGALVALUE || srequested == null) {
            logger.error("TransferId and Requested/Requester HostId must be set");
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
        this.future = future;
        this.specialId = specialId;
        this.requested = requested;
        this.requester = requester;
        this.cancel = cancel;
        this.stop = stop;
        this.restart = restart;
        this.networkTransaction = networkTransaction;
    }


    public void run() {
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(RequestTransfer.class);
        }
        DbTaskRunner runner = null;
        try {
            runner = new DbTaskRunner(DbConstant.admin.session,null,null,
                    specialId,requester,requested);
        } catch (OpenR66DatabaseException e) {
            logger.error("Cannot find the transfer");
            future.setResult(new R66Result(e, null, true,
                    ErrorCode.Internal));
            future.setFailure(e);
            return;
        }
        if (cancel || stop || restart) {
            if (cancel) {
                // Cancel the task and delete any file if in retrieve
                if (runner.isFinished()) {
                    // nothing to do since already finished
                    setDone(runner);
                    logger.warn("Transfer already finished: "+runner.toString());
                    future.setResult(new R66Result(null,true,ErrorCode.TransferOk));
                    future.setSuccess();
                    return;
                } else {
                    // Send a request of cancel
                    sendRequest(LocalPacketFactory.CANCELPACKET);
                }
            } else if (stop) {
                // Just stop the task
                // Send a request
                sendRequest(LocalPacketFactory.STOPPACKET);
            } else if (restart) {
                // Restart if already stopped and not finished
                if (runner.getStatus() != ErrorCode.CompleteOk) {
                    // restart
                    switch (TASKSTEP.values()[runner.getGloballaststep()]) {
                        case PRETASK:
                            // restart
                            runner.setPreTask(0);
                            runner.setExecutionStatus(ErrorCode.InitOk);
                            break;
                        case TRANSFERTASK:
                            // continue
                            runner.setTransferTask(runner.getRank());
                            runner.setExecutionStatus(ErrorCode.PreProcessingOk);
                            break;
                        case POSTTASK:
                            // restart
                            runner.setPostTask(0);
                            runner.setExecutionStatus(ErrorCode.TransferOk);
                            break;
                    }
                    runner.changeUpdatedInfo(UpdatedInfo.UPDATED);
                    try {
                        runner.saveStatus();
                    } catch (OpenR66RunnerErrorException e) {
                        future.setResult(new R66Result(e,null,true,ErrorCode.Internal));
                        future.cancel();
                        return;
                    }
                    future.setResult(new R66Result(null,true,runner.getStatus()));
                    future.setSuccess();
                } else {
                    // Already finished so DONE
                    setDone(runner);
                    future.setResult(new R66Result(null,true,runner.getStatus()));
                    future.cancel();
                }
                logger.warn("Transfer: "+runner.toString());
            }
        } else {
            // Only request
            logger.warn("Transfer: "+runner.toString());
            future.setResult(new R66Result(null,true,runner.getStatus()));
            future.setSuccess();
        }
    }
    /**
     * Set the runner to DONE
     * @param runner
     */
    private void setDone(DbTaskRunner runner) {
        if (runner.getUpdatedInfo() != UpdatedInfo.DONE) {
            runner.changeUpdatedInfo(UpdatedInfo.DONE);
            try {
                runner.saveStatus();
            } catch (OpenR66RunnerErrorException e) {
            }
        }
    }
    private void sendRequest(byte code) {
        DbHostAuth host;
        host = R66Auth.getServerAuth(DbConstant.admin.session,
                    this.requester);
        SocketAddress socketAddress = host.getSocketAddress();
        boolean isSSL = host.isSsl();

        LocalChannelReference localChannelReference = networkTransaction
            .createConnectionWithRetry(socketAddress,isSSL,future);
        socketAddress = null;
        if (localChannelReference == null) {
            logger.warn("Cannot connect to "+host.toString());
            host = null;
            logger.error("Cannot Connect");
            future.setResult(new R66Result(null, true,
                    ErrorCode.ConnectionImpossible));
            future.cancel();
            return;
       }
        ValidPacket packet = new ValidPacket("Request on Transfer",
                this.requested+" "+this.requester+" "+this.specialId,
                code);
        try {
            ChannelUtils.writeAbstractLocalPacket(localChannelReference, packet);
        } catch (OpenR66ProtocolPacketException e) {
            logger.warn("Cannot transfer request to "+host.toString());
            Channels.close(localChannelReference.getLocalChannel());
            localChannelReference = null;
            host = null;
            packet = null;
            logger.error("Bad Protocol", e);
            future.setResult(new R66Result(e, null, true,
                    ErrorCode.TransferError));
            future.setFailure(e);
            return;
        }
        packet = null;
        host = null;
        future.awaitUninterruptibly();
        logger.info("Request done with "+(future.isSuccess()?"success":"error"));

        Channels.close(localChannelReference.getLocalChannel());
        localChannelReference = null;
    }
    /**
     * @param args
     */
    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(RequestTransfer.class);
        }
        if (! getParams(args)) {
            logger.error("Wrong initialization");
            if (DbConstant.admin != null && DbConstant.admin.isConnected) {
                try {
                    DbConstant.admin.close();
                } catch (OpenR66DatabaseSqlError e) {
                }
            }
            System.exit(1);
        }
        try {
            Configuration.configuration.pipelineInit();
            NetworkTransaction networkTransaction = new NetworkTransaction();
            R66Future result = new R66Future(true);
            RequestTransfer requestTransfer =
                new RequestTransfer(result, sspecialId, srequested, srequester,
                        scancel, sstop, srestart,
                        networkTransaction);
            requestTransfer.run();
            result.awaitUninterruptibly();
            // FIXME use result
            if (result.isSuccess()) {
                logger.warn("Success: " +
                        result.getResult().toString());
            } else {
                logger.error("Error: " +
                        result.getResult().toString());
            }

        } finally {
            if (DbConstant.admin != null) {
                try {
                    DbConstant.admin.close();
                } catch (OpenR66DatabaseSqlError e) {
                }
            }
        }
    }

}
