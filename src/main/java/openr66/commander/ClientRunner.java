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

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.netty.channel.Channels;

import openr66.context.authentication.R66Auth;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.DbConstant;
import openr66.database.data.AbstractDbData;
import openr66.database.data.DbHostAuth;
import openr66.database.data.DbTaskRunner;
import openr66.database.data.AbstractDbData.UpdatedInfo;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
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
            logger.error("No connection Error", e);
            return;
        } catch (OpenR66ProtocolPacketException e) {
            logger.error("Protocol Error", e);
            return;
        }
        logger.warn("Result: "+transfer.isSuccess()+" "+transfer.getResult().toString());
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
     */
    public R66Future runTransfer() throws OpenR66RunnerErrorException, OpenR66ProtocolNoConnectionException, OpenR66ProtocolPacketException {
        this.changeUpdatedInfo(UpdatedInfo.TORUN);
        long id = taskRunner.getSpecialId();
        String tid;
        if (id == DbConstant.ILLEGALVALUE) {
            tid = taskRunner.getRuleId()+"_"+taskRunner.getMode()+"_NEWTRANSFER";
        } else {
            tid = taskRunner.getRuleId()+"_"+taskRunner.getMode()+"_"+id;
        }
        Thread.currentThread().setName(tid);
        logger.info("Will run "+this.taskRunner.toString());

        DbHostAuth host;
        try {
            host = R66Auth.getServerAuth(DbConstant.admin.session,
                    taskRunner.getRequested());
        } catch (OpenR66RunnerErrorException e1) {
            // Don't have to restart a task for itself (or should use requester)
            logger.warn("Requested host cannot initiate itself the request", e1);
            this.changeUpdatedInfo(UpdatedInfo.INERROR);
            throw e1;
        }
        if (host == null) {
            logger.warn("Requested host cannot be found: "+taskRunner.getRequested());
            this.changeUpdatedInfo(UpdatedInfo.INERROR);
            throw new OpenR66RunnerErrorException("Requested host cannot be found");
        }
        SocketAddress socketAddress = host.getSocketAddress();

        LocalChannelReference localChannelReference = networkTransaction
            .createConnectionWithRetry(socketAddress, futureRequest);
        socketAddress = null;
        if (localChannelReference == null) {
            // propose to redo
            // See if reprogramming is ok (not too many tries)
            if (incrementTaskRunerTry(taskRunner, Configuration.RETRYNB)) {
                logger.warn("Will retry since Cannot connect to "+host.toString());
                this.changeUpdatedInfo(UpdatedInfo.UPDATED);
            } else {
                logger.warn("Will not retry since limit of connection attemtps is reached for "+
                        host.toString());
                this.changeUpdatedInfo(UpdatedInfo.TORUN);
            }
            host = null;
            throw new OpenR66ProtocolNoConnectionException("Cannot connect to server");
        }

        if (taskRunner.getRank() > 0) {
            // start from one rank before
            taskRunner.setRankAtStartup(taskRunner.getRank()-1);
        }
        RequestPacket request = taskRunner.getRequest();
        logger.info("Will send request "+request.toString());
        try {
            ChannelUtils.writeAbstractLocalPacket(localChannelReference, request);
        } catch (OpenR66ProtocolPacketException e) {
            // propose to redo
            logger.warn("Cannot transfer request to "+host.toString());
            this.changeUpdatedInfo(UpdatedInfo.INERROR);
            Channels.close(localChannelReference.getLocalChannel());
            localChannelReference = null;
            host = null;
            request = null;
            throw e;
        }
        logger.info("Wait for request to "+host.toString());
        request = null;
        host = null;
        R66Future transfer = localChannelReference.getFutureRequest();
        transfer.awaitUninterruptibly();
        logger.info("Request done with "+(transfer.isSuccess()?"success":"error"));

        Channels.close(localChannelReference.getLocalChannel());
        localChannelReference = null;
        // now reload TaskRunner if it still exists (light client can forget it)
        if (transfer.isSuccess()) {
            try {
                taskRunner.select();
                this.changeUpdatedInfo(UpdatedInfo.DONE);
            } catch (OpenR66DatabaseException e) {
                logger.info("Not a problem but cannot find at the end the task");
            }
        } else {
            try {
                taskRunner.select();
                this.changeUpdatedInfo(UpdatedInfo.INERROR);
            } catch (OpenR66DatabaseException e) {
                logger.info("Not a problem but cannot find at the end the task");
            }
        }
        return transfer;
    }
    /**
     * Change the UpdatedInfo of the current runner
     * @param info
     */
    public void changeUpdatedInfo(AbstractDbData.UpdatedInfo info) {
        this.taskRunner.changeUpdatedInfo(info);
        try {
            this.taskRunner.update();
        } catch (OpenR66DatabaseException e) {
        }
    }

}
