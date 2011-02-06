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
package openr66.client;

import goldengate.common.database.exception.GoldenGateDatabaseException;
import goldengate.common.file.DataBlock;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.commander.ClientRunner;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.DbConstant;
import openr66.database.data.DbRule;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66DatabaseGlobalException;
import openr66.protocol.exception.OpenR66Exception;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolNotYetConnectionException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.RetrieveRunner;
import openr66.protocol.localhandler.packet.EndRequestPacket;
import openr66.protocol.localhandler.packet.ErrorPacket;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.test.TestSendThroughClient;
import openr66.protocol.utils.ChannelUtils;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;

/**
 * Class for Send Through client
 *
 * This class does not included the real file transfer since it is up to the business project
 * to implement how to read new data to be sent to the remote host. If an error occurs,
 * no transfer log is kept.
 *
 * 1) Configuration must have been loaded<br>
 * <br>
 * 2) Pipeline and NetworkTransaction must have been initiated:<br>
 * <tt>     Configuration.configuration.pipelineInit();</tt><br>
 * <tt>     NetworkTransaction networkTransaction = new NetworkTransaction();</tt><br>
 * <br>
 * 3) Prepare the request of transfer:<br>
 * <tt>     R66Future futureReq = new R66Future(true);</tt><br>
 * <tt>     SendThroughClient transaction = new SendThroughClient(futureReq,...);</tt><br>
 * <tt>     if (! transaction.initiateRequest()) { error }</tt><br>
 * <br>
 * 4) Once initiateRequest() gives true, you are ready to send the data in through mode like:<br>
 * <tt>     byte[] data = readOrGetInSomeWayData();</tt><br>
 * <tt>     DataBlock block = transaction.transformToDataBlock(data);</tt><br>
 * <tt>     futureWrite = transaction.writeWhenPossible(block);</tt><br>
 * <br>
 * 5) Once you have finished, so this is the last block, you have to do the following:<br>
 * If the last block is not empty:<br>
 * <tt>     DataBlock block = transaction.transformToDataBlock(data);</tt><br>
 * <tt>     block.setEOF(true);</tt><br>
 * Or if the last block is empty:<br>
 * <tt>     DataBlock block = transaction.transformToDataBlock(null);</tt><br>
 * Then <br>
 * <tt>     futureWrite = transaction.writeWhenPossible(block);</tt><br>
 * <tt>     futureWrite.awaitUninterruptibly();</tt><br>
 * <br>
 * 6) If everything is in success:<br>
 * <tt>     transaction.finalizeRequest();</tt><br>
 * <br>
 * And now wait for the transfer to finish:<br>
 * <tt>     futureReq.awaitUninterruptibly();</tt><br>
 * <tt>     R66Result result = futureReq.getResult();</tt><br>
 * <br>
 * 7) If there is the need to re-do, just re-execute the steps from 3 to 6.<br>
 * Don't forget at the very end to finish the global structure (steps 3 to 6 no more executed):<br>
 * <tt>     networkTransaction.closeAll();</tt><br>
 * <br>
 * 8) In case of errors during steps 4 or 5 (and only those), call the following:<br>
 * <tr>     transaction.transferInError(openR66Exception);</tr><br>
 * <br>
 * @see TestSendThroughClient {@link TestSendThroughClient} Class as example of usage
 *
 * @author Frederic Bregier
 *
 */
public abstract class SendThroughClient extends AbstractTransfer {
    protected final NetworkTransaction networkTransaction;
    protected LocalChannelReference localChannelReference;
    protected DbTaskRunner taskRunner = null;
    /**
     * @param future
     * @param remoteHost
     * @param filename
     * @param rulename
     * @param fileinfo
     * @param isMD5
     * @param blocksize
     * @param networkTransaction
     * @param id
     */
    public SendThroughClient(R66Future future, String remoteHost,
            String filename, String rulename, String fileinfo, boolean isMD5,
            int blocksize, long id, NetworkTransaction networkTransaction) {
        super(SendThroughClient.class,
                future, filename, rulename, fileinfo, isMD5, remoteHost, blocksize, id, null);
        this.networkTransaction = networkTransaction;
    }
    /**
     * DO NOT CALL THIS!
     */
    public void run() {
        logger.error("DO NOT call this method for this class");
    }
    /**
     * Prior to call this method, the pipeline and NetworkTransaction must have been initialized.
     * It is the responsibility of the caller to finish all network resources.
     * Note that this is only the first part of the execution for this client.
     *
     * @return True if the initiate of the request is OK, else False
     */
    public boolean initiateRequest() {
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(SendThroughClient.class);
        }
        DbRule rule;
        try {
            rule = new DbRule(DbConstant.admin.session, rulename);
        } catch (GoldenGateDatabaseException e) {
            logger.error("Cannot get Rule: "+rulename, e);
            future.setResult(new R66Result(new OpenR66DatabaseGlobalException(e), null, true,
                    ErrorCode.Internal,null));
            future.setFailure(e);
            return false;
        }
        int mode = rule.mode;
        if (isMD5) {
            mode = RequestPacket.getModeMD5(mode);
        }
        RequestPacket request = new RequestPacket(rulename,
                mode, filename, blocksize, 0,
                id, fileinfo);
        // Not isRecv since it is the requester, so send => isSender is true
        boolean isSender = true;
        try {
            try {
                // starttime since immediate
                taskRunner =
                    new DbTaskRunner(DbConstant.admin.session,rule,isSender,request,remoteHost, null);
            } catch (GoldenGateDatabaseException e) {
                logger.error("Cannot get task", e);
                future.setResult(new R66Result(new OpenR66DatabaseGlobalException(e), null, true,
                        ErrorCode.Internal, null));
                future.setFailure(e);
                return false;
            }
            ClientRunner runner = new ClientRunner(networkTransaction, taskRunner, future);
            OpenR66ProtocolNotYetConnectionException exc = null;
            for (int i = 0; i < Configuration.RETRYNB; i++) {
                try {
                    localChannelReference = runner.initRequest();
                    exc = null;
                    break;
                } catch (OpenR66RunnerErrorException e) {
                    logger.error("Cannot Transfer", e);
                    future.setResult(new R66Result(e, null, true,
                            ErrorCode.Internal, taskRunner));
                    future.setFailure(e);
                    return false;
                } catch (OpenR66ProtocolNoConnectionException e) {
                    logger.error("Cannot Connect", e);
                    future.setResult(new R66Result(e, null, true,
                            ErrorCode.ConnectionImpossible, taskRunner));
                    future.setFailure(e);
                    return false;
                } catch (OpenR66ProtocolPacketException e) {
                    logger.error("Bad Protocol", e);
                    future.setResult(new R66Result(e, null, true,
                            ErrorCode.TransferError, taskRunner));
                    future.setFailure(e);
                    return false;
                } catch (OpenR66ProtocolNotYetConnectionException e) {
                    logger.info("Not Yet Connected", e);
                    exc = e;
                    continue;
                }
            }
            if (exc!= null) {
                logger.error("Cannot Connect", exc);
                future.setResult(new R66Result(exc, null, true,
                        ErrorCode.ConnectionImpossible, taskRunner));
                future.setFailure(exc);
                return false;
            }
            try {
                localChannelReference.waitReadyForSendThrough();
            } catch (OpenR66Exception e) {
                logger.error("Cannot Transfer", e);
                future.setResult(new R66Result(e, null, true,
                        ErrorCode.Internal, taskRunner));
                future.setFailure(e);
                return false;
            }
            // now start the send from external data
            return true;
        } finally {
            if (taskRunner != null) {
                if (future.isFailed()) {
                    try {
                        taskRunner.delete();
                    } catch (GoldenGateDatabaseException e) {
                    }
                }
            }
        }
    }
    /**
     * Finalize the request
     */
    public void finalizeRequest() {
        try {
            try {
                ChannelUtils.writeEndTransfer(localChannelReference);
            } catch (OpenR66ProtocolPacketException e) {
                // An error occurs!
                try {
                    localChannelReference.getSession().setFinalizeTransfer(
                            false,
                            new R66Result(e, localChannelReference.getSession(), false,
                                    ErrorCode.Internal, taskRunner));
                } catch (OpenR66RunnerErrorException e1) {
                    transferInError(e1);
                    return;
                } catch (OpenR66ProtocolSystemException e1) {
                    transferInError(e1);
                    return;
                }
            }
            localChannelReference.getFutureEndTransfer().awaitUninterruptibly();
            logger.info("Await future End Transfer done: " +
                    localChannelReference.getFutureEndTransfer().isSuccess());
            if (localChannelReference.getFutureEndTransfer().isSuccess()) {
                // send a validation
                EndRequestPacket validPacket = new EndRequestPacket(ErrorCode.CompleteOk.ordinal());
                try {
                    ChannelUtils.writeAbstractLocalPacket(localChannelReference, validPacket)
                        .awaitUninterruptibly();
                } catch (OpenR66ProtocolPacketException e) {
                }
                if (!localChannelReference.getFutureRequest().awaitUninterruptibly(
                    Configuration.configuration.TIMEOUTCON)) {
                    // valid it however
                    localChannelReference.validateRequest(localChannelReference
                        .getFutureEndTransfer().getResult());
                }
                if (taskRunner != null && taskRunner.isSelfRequested()) {
                    ChannelUtils.close(localChannelReference.getLocalChannel());
                }
            } else {
                transferInError(null);
            }
        } finally {
            if (taskRunner != null) {
                if ((!future.isSuccess()) || nolog) {
                    try {
                        taskRunner.delete();
                    } catch (GoldenGateDatabaseException e) {
                    }
                }
            }
        }
    }
    /**
     * To be used in case of error after a correct initiate of the request
     * @param e
     */
    public void transferInError(OpenR66Exception e) {
        if (!localChannelReference.getFutureEndTransfer().getResult().isAnswered) {
            R66Result result = new R66Result(e, localChannelReference.getSession(), true,
                    ErrorCode.TransferError, taskRunner);
            logger.error("Transfer in error", e);
            ErrorPacket error = new ErrorPacket("Transfer in error",
                    ErrorCode.TransferError.getCode(), ErrorPacket.FORWARDCLOSECODE);
            try {
                ChannelUtils.writeAbstractLocalPacket(localChannelReference, error).awaitUninterruptibly();
            } catch (OpenR66ProtocolPacketException e1) {
            }
            localChannelReference.invalidateRequest(result);
        }
        ChannelUtils.close(localChannelReference.getLocalChannel());
    }
    /**
     * Write the next block when the channel is ready to prevent OOM
     * @param block
     * @return the ChannelFuture on the write operation
     *
     * @throws OpenR66RunnerErrorException
     * @throws OpenR66ProtocolPacketException
     * @throws OpenR66ProtocolSystemException
     */
    public ChannelFuture writeWhenPossible(DataBlock block)
    throws OpenR66RunnerErrorException, OpenR66ProtocolPacketException, OpenR66ProtocolSystemException {
        return RetrieveRunner.writeWhenPossible(block, localChannelReference);
    }
    /**
     * Utility method for send through mode
     * @param data the data byte, if null it is the last block
     * @return the DataBlock associated to the data
     */
    public DataBlock transformToDataBlock(byte []data) {
        DataBlock block = new DataBlock();
        if (data == null) {
            // last block
            block.setEOF(true);
        } else {
            block.setBlock(ChannelBuffers.wrappedBuffer(data));
        }
        return block;
    }
}
