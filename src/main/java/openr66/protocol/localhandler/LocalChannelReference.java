/**
 * This file is part of GoldenGate Project (named also GoldenGate or GG).
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All GoldenGate Project is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 * 
 * GoldenGate is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with GoldenGate . If not,
 * see <http://www.gnu.org/licenses/>.
 */
package openr66.protocol.localhandler;

import goldengate.common.database.DbSession;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.client.RecvThroughHandler;
import openr66.commander.ClientRunner;
import openr66.context.ErrorCode;
import openr66.context.R66FiniteDualStates;
import openr66.context.R66Result;
import openr66.context.R66Session;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.DbConstant;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66Exception;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.networkhandler.NetworkChannel;
import openr66.protocol.networkhandler.NetworkServerHandler;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.channel.Channel;

/**
 * Reference of one object using Local Channel localId and containing local channel and network
 * channel.
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
	 * Associated NetworkChannel
	 */
	private NetworkChannel networkChannelObject;

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
	 * Future on Request
	 */
	private final R66Future futureRequest;

	/**
	 * Future on Valid Starting Request
	 */
	private R66Future futureValidRequest = new R66Future(true);

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
	 * Last error message
	 */
	private String errorMessage = "NoError";

	/**
	 * Last error code
	 */
	private ErrorCode code = ErrorCode.Unknown;

	/**
	 * RecvThroughHandler
	 */
	private RecvThroughHandler recvThroughHandler;

	/**
	 * Thread for ClientRunner if any
	 */
	private ClientRunner clientRunner = null;

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
	 * @return the networkChannelObject
	 */
	public NetworkChannel getNetworkChannelObject() {
		return networkChannelObject;
	}

	/**
	 * @param networkChannelObject
	 *            the networkChannelObject to set
	 */
	public void setNetworkChannelObject(NetworkChannel networkChannelObject) {
		this.networkChannelObject = networkChannelObject;
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
	 * @return the current errorMessage
	 */
	public String getErrorMessage() {
		return errorMessage;
	}

	/**
	 * @param errorMessage
	 *            the errorMessage to set
	 */
	public void setErrorMessage(String errorMessage, ErrorCode code) {
		this.errorMessage = errorMessage;
		this.code = code;
	}

	/**
	 * @return the code
	 */
	public ErrorCode getCurrentCode() {
		return code;
	}

	/**
	 * Validate or not the Startup (before connection)
	 * 
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
			if (!futureStartup.await(Configuration.configuration.TIMEOUTCON)) {
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
			setErrorMessage(result.getMessage(), result.code);
			futureConnection.cancel();
		}
	}

	/**
	 * 
	 * @return the futureValidateConnection
	 */
	public R66Future getFutureValidateConnection() {
		try {
			if (!futureConnection.await(Configuration.configuration.TIMEOUTCON)) {
				R66Result result;
				if (futureConnection.isDone()) {
					if (futureRequest.isDone()) {
						result = futureRequest.getResult();
					} else {
						result = new R66Result(
								new OpenR66ProtocolNoConnectionException(
										"Connection was impossible"), session,
								false, ErrorCode.ConnectionImpossible, null);
					}
				} else {
					result = new R66Result(
							new OpenR66ProtocolNoConnectionException(
									"Out of time"), session, false,
							ErrorCode.ConnectionImpossible, null);
				}
				validateConnection(false, result);
				return futureConnection;
			}
		} catch (InterruptedException e) {
			R66Result result = new R66Result(
					new OpenR66ProtocolNoConnectionException(
							"Interrupted connection"), session, false,
					ErrorCode.ConnectionImpossible, null);
			validateConnection(false, result);
			return futureConnection;
		}
		return futureConnection;
	}

	/**
	 * Validate the End of a Transfer
	 * 
	 * @param finalValue
	 */
	public void validateEndTransfer(R66Result finalValue) {
		if (!futureEndTransfer.isDone()) {
			futureEndTransfer.setResult(finalValue);
			futureEndTransfer.setSuccess();
		} else {
			logger.debug("Could not validate since Already validated: " +
					futureEndTransfer.isSuccess() + " " + finalValue);
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
	 * 
	 * @throws OpenR66Exception
	 */
	public void waitReadyForSendThrough() throws OpenR66Exception {
		logger.debug("Wait for End of Prepare Transfer");
		try {
			this.futureEndTransfer.await();
		} catch (InterruptedException e) {
			throw new OpenR66RunnerErrorException("Interrupted", e);
		}
		if (this.futureEndTransfer.isSuccess()) {
			// reset since transfer will start now
			this.futureEndTransfer = new R66Future(true);
		} else {
			throw this.futureEndTransfer.getResult().exception;
		}
	}

	/**
	 * @return the futureValidRequest
	 */
	public R66Future getFutureValidRequest() {
		return futureValidRequest;
	}

	/**
	 * @return the futureRequest
	 */
	public R66Future getFutureRequest() {
		return futureRequest;
	}

	/**
	 * Invalidate the current request
	 * 
	 * @param finalvalue
	 */
	public void invalidateRequest(R66Result finalvalue) {
		R66Result finalValue = finalvalue;
		if (finalValue == null) {
			finalValue = new R66Result(session, false, ErrorCode.Unknown, this.session.getRunner());
		}
		logger.debug("FET: " + futureEndTransfer.isDone() + ":" +
				futureEndTransfer.isSuccess() + " FVR: " +
				futureValidRequest.isDone() + ":" +
				futureValidRequest.isSuccess() + " FR: " +
				futureRequest.isDone() + ":" + futureRequest.isSuccess() + " " +
				finalValue.getMessage());
		if (!futureEndTransfer.isDone()) {
			futureEndTransfer.setResult(finalValue);
			if (finalValue.exception != null) {
				futureEndTransfer.setFailure(finalValue.exception);
			} else {
				futureEndTransfer.cancel();
			}
		}
		if (!futureValidRequest.isDone()) {
			futureValidRequest.setResult(finalValue);
			if (finalValue.exception != null) {
				futureValidRequest.setFailure(finalValue.exception);
			} else {
				futureValidRequest.cancel();
			}
		}
		logger.debug("Invalidate Request", new Exception(
				"Trace for Invalidation"));
		if (finalValue.code != ErrorCode.ServerOverloaded) {
			if (!futureRequest.isDone()) {
				setErrorMessage(finalValue.getMessage(), finalValue.code);
				futureRequest.setResult(finalValue);
				if (finalValue.exception != null) {
					futureRequest.setFailure(finalValue.exception);
				} else {
					futureRequest.cancel();
				}
			} else {
				logger.debug("Could not invalidate since Already finished: " +
						futureEndTransfer.getResult());
			}
		} else {
			setErrorMessage(finalValue.getMessage(), finalValue.code);
			logger.debug("Overloaded");
		}
		if (this.session != null) {
			DbTaskRunner runner = this.session.getRunner();
			if (runner != null) {
				if (runner.isSender()) {
					NetworkTransaction.stopRetrieve(this);
				}
			}
		}
	}

	/**
	 * Validate the current Request
	 * 
	 * @param finalValue
	 */
	public void validateRequest(R66Result finalValue) {
		setErrorMessage("NoError", null);
		if (!futureEndTransfer.isDone()) {
			logger.debug("Will validate EndTransfer");
			validateEndTransfer(finalValue);
		}
		if (!futureValidRequest.isDone()) {
			futureValidRequest.setResult(finalValue);
			futureValidRequest.setSuccess();
		}
		logger.debug("Validate Request");
		if (!futureRequest.isDone()) {
			futureRequest.setResult(finalValue);
			futureRequest.setSuccess();
		} else {
			logger.debug("Already validated: " + futureRequest.isSuccess() +
					" " + finalValue);
			if (!futureRequest.getResult().isAnswered) {
				futureRequest.getResult().isAnswered = finalValue.isAnswered;
			}
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
	 * @param recvThroughHandler
	 *            the recvThroughHandler to set
	 */
	public void setRecvThroughHandler(RecvThroughHandler recvThroughHandler) {
		this.recvThroughHandler = recvThroughHandler;
	}

	/**
	 * @return the clientRunner
	 */
	public ClientRunner getClientRunner() {
		return clientRunner;
	}

	/**
	 * @param clientRunner
	 *            the clientRunner to set
	 */
	public void setClientRunner(ClientRunner clientRunner) {
		this.clientRunner = clientRunner;
	}

	/**
	 * Shortcut to set a new state in Session
	 * 
	 * @param desiredState
	 */
	public void sessionNewState(R66FiniteDualStates desiredState) {
		if (session != null) {
			session.newState(desiredState);
		}
	}

}
