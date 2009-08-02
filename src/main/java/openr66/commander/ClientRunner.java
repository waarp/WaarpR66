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

import org.jboss.netty.channel.Channels;

import openr66.context.R66Result;
import openr66.context.authentication.R66Auth;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.DbConstant;
import openr66.database.data.AbstractDbData;
import openr66.database.data.DbHostAuth;
import openr66.database.data.DbTaskRunner;
import openr66.database.data.AbstractDbData.UpdatedInfo;
import openr66.database.exception.OpenR66DatabaseException;
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

    private final NetworkTransaction networkTransaction;
    private final DbTaskRunner taskRunner;

    public ClientRunner(NetworkTransaction networkTransaction, DbTaskRunner taskRunner) {
        this.networkTransaction = networkTransaction;
        this.taskRunner = taskRunner;
    }
    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
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
            this.changeUpdatedInfo(UpdatedInfo.UNKNOWN);
            return;
        }
        SocketAddress socketAddress = host.getSocketAddress();

        LocalChannelReference localChannelReference = networkTransaction
            .createConnectionWithRetry(socketAddress);
        socketAddress = null;
        if (localChannelReference == null) {
            // propose to redo
            logger.warn("Cannot connect to "+host.toString());
            this.changeUpdatedInfo(UpdatedInfo.UPDATED);
            host = null;
            return;
        }

        RequestPacket request = taskRunner.getRequest();
        logger.info("Will send request "+request.toString());
        try {
            ChannelUtils.writeAbstractLocalPacket(localChannelReference, request);
        } catch (OpenR66ProtocolPacketException e) {
            // propose to redo
            logger.warn("Cannot transfer request to "+host.toString());
            this.changeUpdatedInfo(UpdatedInfo.UPDATED);
            Channels.close(localChannelReference.getLocalChannel());
            localChannelReference = null;
            host = null;
            request = null;
            return;
        }
        logger.info("Wait for request to "+host.toString());
        request = null;
        host = null;
        R66Future transfer = localChannelReference.getFutureRequest();
        transfer.awaitUninterruptibly();
        logger.info("Request done with "+(transfer.isSuccess()?"success":"error"));

        // FIXME TODO Auto-generated method stub
        R66Result result = transfer.getResult();
        Channels.close(localChannelReference.getLocalChannel());
        localChannelReference = null;
        // now reload TaskRunner if it still exists (light client can forget it)
        if (transfer.isSuccess()) {
            try {
                taskRunner.select();
                this.changeUpdatedInfo(UpdatedInfo.DONE);
            } catch (OpenR66DatabaseException e) {
                logger.warn("Not a problem but cannot find at the end the task", e);
            }
        }
        logger.warn("Result: "+transfer.isSuccess()+" "+result.toString());
        transfer = null;
        result = null;
    }
    /**
     *
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
