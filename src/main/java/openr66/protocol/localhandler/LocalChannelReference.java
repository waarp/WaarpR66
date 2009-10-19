/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors. This is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of the License,
 * or (at your option) any later version. This software is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * software; if not, write to the Free Software Foundation, Inc., 51 Franklin
 * St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF site:
 * http://www.fsf.org.
 */
package openr66.protocol.localhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.client.RecvThroughHandler;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.R66Session;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.DbConstant;
import openr66.database.DbSession;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66Exception;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.networkhandler.NetworkServerHandler;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.channel.Channel;

/**
 * Reference of one object using Local Channel localId and containing local
 * channel and network channel.
 *
 * @author Frederic Bregier
 */
public class LocalChannelReference {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(LocalChannelReference.class);

    /**
     * Local Channel
     */
    private final Channel localChannel;
    /**
     * Network Channel
     */
    private final Channel networkChannel;
    /**
     * Network Server Handler
     */
    private final NetworkServerHandler networkServerHandler;
    /**
     * Local Id
     */
    private final Integer localId;
    /**
     * Remote Id
     */
    private Integer remoteId;
    /**
     * Says that request is done but waiting for EndRequest
     */
    private boolean requestDone = false;
    /**
     * Future on Request
     */
    private final R66Future futureRequest;
    /**
     * Future on Transfer
     */
    private R66Future futureEndTransfer = new R66Future(true);
    /**
     * Future on Connection
     */
    private final R66Future futureConnection = new R66Future(true);
    /**
     * Future on Startup
     */
    private final R66Future futureStartup = new R66Future(true);
    /**
     * Session
     */
    private R66Session session;
    /**
     * RecvThroughHandler
     */
    private RecvThroughHandler recvThroughHandler;

    /**
    *
    * @param localChannel
    * @param networkChannel
    * @param remoteId
    * @param futureRequest
    */
   public LocalChannelReference(Channel localChannel, Channel networkChannel,
           Integer remoteId, R66Future futureRequest) {
       this.localChannel = localChannel;
       this.networkChannel = networkChannel;
       networkServerHandler = (NetworkServerHandler) this.networkChannel
               .getPipeline().getLast();
       localId = this.localChannel.getId();
       this.remoteId = remoteId;
       if (futureRequest == null) {
           this.futureRequest = new R66Future(true);
       } else {
           this.futureRequest = futureRequest;
       }
   }
   /**
    * Special empty LCR constructor
    */
   public LocalChannelReference() {
       this.localChannel = null;
       this.networkChannel = null;
       networkServerHandler = null;
       localId = 0;
       this.futureRequest = new R66Future(true);
   }

   /**
     * @return the localChannel
     */
    public Channel getLocalChannel() {
        return localChannel;
    }

    /**
     * @return the networkChannel
     */
    public Channel getNetworkChannel() {
        return networkChannel;
    }

    /**
     * @return the id
     */
    public Integer getLocalId() {
        return localId;
    }

    /**
     * @return the remoteId
     */
    public Integer getRemoteId() {
        return remoteId;
    }

    /**
     * @return the networkServerHandler
     */
    public NetworkServerHandler getNetworkServerHandler() {
        return networkServerHandler;
    }
    /**
     *
     * @return the actual dbSession
     */
    public DbSession getDbSession() {
        if (networkServerHandler != null) {
            return networkServerHandler.getDbSession();
        }
        return DbConstant.admin.session;
    }
    /**
     * @param remoteId
     *            the remoteId to set
     */
    public void setRemoteId(Integer remoteId) {
        this.remoteId = remoteId;
    }

    /**
     * @return the session
     */
    public R66Session getSession() {
        return session;
    }

    /**
     * @param session
     *            the session to set
     */
    public void setSession(R66Session session) {
        this.session = session;
    }

    /**
     * Validate or not the Startup (before connection)
     * @param validate
     */
    public void validateStartup(boolean validate) {
        if (futureStartup.isDone()) {
            return;
        }
        if (validate) {
            futureStartup.setSuccess();
        } else {
            futureStartup.cancel();
        }
    }
    /**
    *
    * @return the futureValidateStartup
    */
   public R66Future getFutureValidateStartup() {
       try {
           if (!futureStartup
                   .await(Configuration.configuration.TIMEOUTCON)) {
               validateStartup(false);
               return futureStartup;
           }
        } catch (InterruptedException e) {
            validateStartup(false);
            return futureStartup;
        }
       return futureStartup;
   }
    /**
     * Validate or Invalidate the connection (authentication)
     *
     * @param validate
     */
    public void validateConnection(boolean validate, R66Result result) {
        if (futureConnection.isDone()) {
            logger.debug("LocalChannelReference already validated: " +
                    futureConnection.isSuccess());
            return;
        }
        logger.debug("LocalChannelReference validate: " + validate);
        if (validate) {
            futureConnection.setResult(result);
            futureConnection.setSuccess();
        } else {
            futureConnection.setResult(result);
            futureConnection.cancel();
        }
    }

    /**
     *
     * @return the futureValidateConnection
     */
    public R66Future getFutureValidateConnection() {
        try {
            if (!futureConnection
                    .await(Configuration.configuration.TIMEOUTCON)) {
                R66Result result = new R66Result(
                        new OpenR66ProtocolNoConnectionException("Out of time"),
                        session, false, ErrorCode.ConnectionImpossible);
                validateConnection(false, result);
                return futureConnection;
            }
        } catch (InterruptedException e) {
            R66Result result = new R66Result(
                    new OpenR66ProtocolNoConnectionException("Interrupted connection"),
                    session, false, ErrorCode.ConnectionImpossible);
            validateConnection(false, result);
            return futureConnection;
        }
        return futureConnection;
    }

    /**
     * Invalidate the current request
     *
     * @param finalValue
     */
    public void invalidateRequest(R66Result finalValue) {
        if (!futureEndTransfer.isDone()) {
            futureEndTransfer.setResult(finalValue);
            if (finalValue.exception != null) {
                futureEndTransfer.setFailure(finalValue.exception);
            } else {
                futureEndTransfer.cancel();
            }
        }
        if (!futureRequest.isDone()) {
            futureRequest.setResult(finalValue);
            if (finalValue.exception != null) {
                futureRequest.setFailure(finalValue.exception);
            } else {
                futureRequest.cancel();
            }
        } else {
            logger.info("Could not invalidate since Already finished: " + futureEndTransfer.getResult());
        }
        DbTaskRunner runner = this.session.getRunner();
        if (runner != null) {
            if (runner.isSender()) {
                NetworkTransaction.stopRetrieve(this);
            }
        }
    }
    /**
     * Validate the End of a Transfer
     * @param finalValue
     */
    public void validateEndTransfer(R66Result finalValue) {
        if (!futureEndTransfer.isDone()) {
            futureEndTransfer.setResult(finalValue);
            futureEndTransfer.setSuccess();
        } else {
            logger.info("Could not validate since Already validated: " + futureEndTransfer.isSuccess() +
                    " " + finalValue);
            if (!futureEndTransfer.getResult().isAnswered) {
                futureEndTransfer.getResult().isAnswered = finalValue.isAnswered;
            }
        }
    }

    /**
     * @return the futureEndTransfer
     */
    public R66Future getFutureEndTransfer() {
        return futureEndTransfer;
    }
    /**
     * Special waiter for Send Through method. It reset the EndTransfer future.
     * @throws OpenR66Exception
     */
    public void waitReadyForSendThrough() throws OpenR66Exception {
        logger.info("Wait for End of Prepare Transfer");
        try {
            this.futureEndTransfer.await();
        } catch (InterruptedException e) {
            throw new OpenR66RunnerErrorException("Interrupted",e);
        }
        if (this.futureEndTransfer.isSuccess()) {
            // reset since transfer will start now
            this.futureEndTransfer = new R66Future(true);
        } else {
            throw this.futureEndTransfer.getResult().exception;
        }
    }
    /**
     * @return the futureRequest
     */
    public R66Future getFutureRequest() {
        return futureRequest;
    }
    /**
     * Set the request as done but still waiting if possible the EndRequest
     */
    public void RequestIsDone() {
        this.requestDone = true;
    }
    /**
     *
     * @return True if the request was in a correct status
     */
    public boolean isRequestDone() {
        return this.requestDone;
    }
    /**
     * Validate the current Request
     * @param finalValue
     */
    public void validateRequest(R66Result finalValue) {
        if (!futureEndTransfer.isDone()) {
            logger.info("Will validate EndTransfer");
            validateEndTransfer(finalValue);
        }
        if (!futureRequest.isDone()) {
            futureRequest.setResult(finalValue);
            futureRequest.setSuccess();
        } else {
            logger.info("Already validated: " + futureRequest.isSuccess() +
                    " " + finalValue);
            if (!futureRequest.getResult().isAnswered) {
                futureRequest.getResult().isAnswered = finalValue.isAnswered;
            }
        }
    }
    /**
     * Set the result as the status of EndOfTransfer if successful, or set as finalValue
     * @param finalValue
     */
    public void mixedValidateRequest(R66Result finalValue) {
        if (getFutureEndTransfer().isSuccess()) {
            validateRequest(getFutureEndTransfer().getResult());
        } else {
            invalidateRequest(finalValue);
        }
    }
    @Override
    public String toString() {
        return "LCR: L: " + localId + " R: " + remoteId;
    }

    /**
     * @return the recvThroughHandler
     */
    public RecvThroughHandler getRecvThroughHandler() {
        return recvThroughHandler;
    }

    /**
     * @param recvThroughHandler the recvThroughHandler to set
     */
    public void setRecvThroughHandler(RecvThroughHandler recvThroughHandler) {
        this.recvThroughHandler = recvThroughHandler;
    }
}
