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
package org.waarp.openr66.protocol.localhandler;

import static org.waarp.openr66.context.R66FiniteDualStates.AUTHENTD;
import static org.waarp.openr66.context.R66FiniteDualStates.AUTHENTR;
import static org.waarp.openr66.context.R66FiniteDualStates.BUSINESSD;
import static org.waarp.openr66.context.R66FiniteDualStates.CLOSEDCHANNEL;
import static org.waarp.openr66.context.R66FiniteDualStates.DATAR;
import static org.waarp.openr66.context.R66FiniteDualStates.ENDREQUESTR;
import static org.waarp.openr66.context.R66FiniteDualStates.ENDREQUESTS;
import static org.waarp.openr66.context.R66FiniteDualStates.ENDTRANSFERR;
import static org.waarp.openr66.context.R66FiniteDualStates.ENDTRANSFERS;
import static org.waarp.openr66.context.R66FiniteDualStates.ERROR;
import static org.waarp.openr66.context.R66FiniteDualStates.INFORMATION;
import static org.waarp.openr66.context.R66FiniteDualStates.REQUESTD;
import static org.waarp.openr66.context.R66FiniteDualStates.REQUESTR;
import static org.waarp.openr66.context.R66FiniteDualStates.SHUTDOWN;
import static org.waarp.openr66.context.R66FiniteDualStates.STARTUP;
import static org.waarp.openr66.context.R66FiniteDualStates.TEST;
import static org.waarp.openr66.context.R66FiniteDualStates.VALID;
import static org.waarp.openr66.context.R66FiniteDualStates.VALIDOTHER;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.waarp.common.command.exception.CommandAbstractException;
import org.waarp.common.command.exception.Reply421Exception;
import org.waarp.common.command.exception.Reply530Exception;
import org.waarp.common.database.DbPreparedStatement;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.database.exception.WaarpDatabaseNoDataException;
import org.waarp.common.database.exception.WaarpDatabaseSqlException;
import org.waarp.common.digest.FilesystemBasedDigest;
import org.waarp.common.exception.FileTransferException;
import org.waarp.common.file.DataBlock;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.role.RoleDefault.ROLE;
import org.waarp.openr66.client.AbstractBusinessRequest;
import org.waarp.openr66.commander.ClientRunner;
import org.waarp.openr66.configuration.AuthenticationFileBasedConfiguration;
import org.waarp.openr66.configuration.RuleFileBasedConfiguration;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.context.authentication.R66Auth;
import org.waarp.openr66.context.filesystem.R66Dir;
import org.waarp.openr66.context.filesystem.R66File;
import org.waarp.openr66.context.task.ExecJavaTask;
import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;
import org.waarp.openr66.context.task.exception.OpenR66RunnerException;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.database.data.DbRule;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66DatabaseGlobalException;
import org.waarp.openr66.protocol.exception.OpenR66Exception;
import org.waarp.openr66.protocol.exception.OpenR66ExceptionTrappedFactory;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessCancelException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessNoWriteBackException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessQueryAlreadyFinishedException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessQueryStillRunningException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessRemoteFileNotFoundException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessStopException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNetworkException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoDataException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoSslException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNotAuthenticatedException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNotYetConnectionException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolShutdownException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;
import org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket;
import org.waarp.openr66.protocol.localhandler.packet.AuthentPacket;
import org.waarp.openr66.protocol.localhandler.packet.BlockRequestPacket;
import org.waarp.openr66.protocol.localhandler.packet.BusinessRequestPacket;
import org.waarp.openr66.protocol.localhandler.packet.ConnectionErrorPacket;
import org.waarp.openr66.protocol.localhandler.packet.DataPacket;
import org.waarp.openr66.protocol.localhandler.packet.EndRequestPacket;
import org.waarp.openr66.protocol.localhandler.packet.EndTransferPacket;
import org.waarp.openr66.protocol.localhandler.packet.ErrorPacket;
import org.waarp.openr66.protocol.localhandler.packet.InformationPacket;
import org.waarp.openr66.protocol.localhandler.packet.LocalPacketFactory;
import org.waarp.openr66.protocol.localhandler.packet.RequestPacket;
import org.waarp.openr66.protocol.localhandler.packet.ShutdownPacket;
import org.waarp.openr66.protocol.localhandler.packet.StartupPacket;
import org.waarp.openr66.protocol.localhandler.packet.TestPacket;
import org.waarp.openr66.protocol.localhandler.packet.ValidPacket;
import org.waarp.openr66.protocol.networkhandler.NetworkChannel;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelCloseTimer;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.FileUtils;
import org.waarp.openr66.protocol.utils.R66Future;
import org.waarp.openr66.protocol.utils.TransferUtils;

/**
 * The local server handler handles real end file operations.
 * 
 * @author frederic bregier
 */
public class LocalServerHandler extends SimpleChannelHandler {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(LocalServerHandler.class);

	/**
	 * Session
	 */
	private volatile R66Session session;
	/**
	 * Local Channel Reference
	 */
	private volatile LocalChannelReference localChannelReference;
	/**
	 * Global Digest in receive
	 */
	private volatile FilesystemBasedDigest globalDigest;
	
	/*
	 * (non-Javadoc)
	 * @see org.jboss.netty.channel.SimpleChannelHandler#channelClosed(org.jboss.
	 * netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelStateEvent)
	 */
	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
		logger.debug("Local Server Channel Closed: {} {}",
				(localChannelReference != null ? localChannelReference
						: "no LocalChannelReference"), (session.getRunner() != null ?
						session.getRunner().toShortString() : "no runner"));
		// clean session objects like files
		DbTaskRunner runner = session.getRunner();
		boolean mustFinalize = true;
		if (localChannelReference != null &&
				localChannelReference.getFutureRequest().isDone()) {
			// already done
		} else {
			if (localChannelReference != null) {
				R66Future fvr = localChannelReference.getFutureValidRequest();
				try {
					fvr.await();
				} catch (InterruptedException e1) {
				}
				if (fvr.isDone()) {
					if (!fvr.isSuccess()) {
						// test if remote server was Overloaded
						if (fvr.getResult().code == ErrorCode.ServerOverloaded) {
							// ignore
							mustFinalize = false;
						}
					}
				}
				logger.debug("Must Finalize: " + mustFinalize);
				if (mustFinalize) {
					session.newState(ERROR);
					R66Result finalValue = new R66Result(
							new OpenR66ProtocolSystemException(
									"Finalize too early at close time But Must Finalize"),
							session, true, ErrorCode.FinalOp, runner); // True since closed
					try {
						tryFinalizeRequest(finalValue);
					} catch (OpenR66Exception e2) {
					}
				}
			}
		}
		if (mustFinalize && runner != null) {
			if (runner.isSelfRequested()) {
				R66Future transfer = localChannelReference.getFutureRequest();
				// Since requested : log
				R66Result result = transfer.getResult();
				if (transfer.isDone() && transfer.isSuccess()) {
					logger.info("TRANSFER REQUESTED RESULT:\n    SUCCESS\n    " +
							(result != null ? result.toString() : "no result"));
				} else {
					logger.error("TRANSFER REQUESTED RESULT:\n    FAILURE\n    " +
							(result != null ? result.toString() : "no result"));
				}
			}
		}
		session.setStatus(50);
		session.newState(CLOSEDCHANNEL);
		session.clear();
		session.setStatus(51);
		if (localChannelReference != null) {
			if (localChannelReference.getDbSession() != null) {
				localChannelReference.getDbSession().endUseConnection();
				logger.debug("End Use Connection");
			}
			String requester =
					(runner != null && runner.isSelfRequested() &&
					localChannelReference.getNetworkChannelObject() != null) ?
							runner.getRequester() : null;
			NetworkTransaction.removeNetworkChannel(localChannelReference
					.getNetworkChannel(), e.getChannel(), requester);
			/*
			 * // Only requested can has a remote client if (runner != null &&
			 * runner.isSelfRequested() && localChannelReference.getNetworkChannelObject() != null
			 * && localChannelReference.getNetworkChannelObject().count <= 0) {
			 * NetworkTransaction.removeClient(runner.getRequester(),
			 * localChannelReference.getNetworkChannelObject()); }
			 */
			session.setStatus(52);
			Configuration.configuration.getLocalTransaction().remove(e.getChannel());
		} else {
			logger
					.error("Local Server Channel Closed but no LocalChannelReference: " +
							e.getChannel().getId());
		}
		// Now if runner is not yet finished, finish it by force
		if (mustFinalize && localChannelReference != null
				&& (!localChannelReference.getFutureRequest().isDone())) {
			R66Result finalValue = new R66Result(
					new OpenR66ProtocolSystemException(
							"Finalize too early at close time while Request not yet finished"),
					session, true, ErrorCode.FinalOp, runner);
			localChannelReference.invalidateRequest(finalValue);
			// In case stop the attached thread if any
			ClientRunner clientRunner = localChannelReference.getClientRunner();
			if (clientRunner != null) {
				try {
					Thread.sleep(Configuration.WAITFORNETOP);
				} catch (InterruptedException e1) {
				}
				clientRunner.interrupt();
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.jboss.netty.channel.SimpleChannelHandler#channelConnected(org.jboss
	 * .netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelStateEvent)
	 */
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
		session = new R66Session();
		session.setStatus(60);
	}

	/*
	 * (non-Javadoc)
	 * @see org.jboss.netty.channel.SimpleChannelHandler#messageReceived(org.jboss
	 * .netty.channel.ChannelHandlerContext, org.jboss.netty.channel.MessageEvent)
	 */
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws OpenR66Exception {
		// action as requested and answer if necessary
		final AbstractLocalPacket packet = (AbstractLocalPacket) e.getMessage();
		if (packet.getType() == LocalPacketFactory.STARTUPPACKET) {
			startup(e.getChannel(), (StartupPacket) packet);
		} else {
			if (localChannelReference == null) {
				logger.error("No LocalChannelReference at " +
						packet.getClass().getName());
				session.newState(ERROR);
				final ErrorPacket errorPacket = new ErrorPacket(
						"No LocalChannelReference at " +
								packet.getClass().getName(),
						ErrorCode.ConnectionImpossible.getCode(),
						ErrorPacket.FORWARDCLOSECODE);
				try {
					Channels.write(e.getChannel(), errorPacket).await();
				} catch (InterruptedException e1) {
				}
				localChannelReference.invalidateRequest(new R66Result(
						new OpenR66ProtocolSystemException(
								"No LocalChannelReference"), session, true,
						ErrorCode.ConnectionImpossible, null));
				ChannelUtils.close(e.getChannel());
				if (Configuration.configuration.r66Mib != null) {
					Configuration.configuration.r66Mib.notifyWarning(
							"No LocalChannelReference", packet.getClass().getSimpleName());
				}
				return;
			}
			switch (packet.getType()) {
				case LocalPacketFactory.AUTHENTPACKET: {
					authent(e.getChannel(), (AuthentPacket) packet);
					break;
				}
				// Already done case LocalPacketFactory.STARTUPPACKET:
				case LocalPacketFactory.DATAPACKET: {
					session.newState(DATAR);
					data(e.getChannel(), (DataPacket) packet);
					break;
				}
				case LocalPacketFactory.VALIDPACKET: {
					valid(e.getChannel(), (ValidPacket) packet);
					break;
				}
				case LocalPacketFactory.ERRORPACKET: {
					session.newState(ERROR);
					errorMesg(e.getChannel(), (ErrorPacket) packet);
					break;
				}
				case LocalPacketFactory.CONNECTERRORPACKET: {
					connectionError(e.getChannel(),
							(ConnectionErrorPacket) packet);
					break;
				}
				case LocalPacketFactory.REQUESTPACKET: {
					request(e.getChannel(), (RequestPacket) packet);
					break;
				}
				case LocalPacketFactory.SHUTDOWNPACKET: {
					session.newState(SHUTDOWN);
					shutdown(e.getChannel(), (ShutdownPacket) packet);
					break;
				}
				case LocalPacketFactory.STOPPACKET:
				case LocalPacketFactory.CANCELPACKET:
				case LocalPacketFactory.CONFIMPORTPACKET:
				case LocalPacketFactory.CONFEXPORTPACKET:
				case LocalPacketFactory.BANDWIDTHPACKET: {
					logger.error("Unimplemented Mesg: " +
							packet.getClass().getName());
					session.newState(ERROR);
					localChannelReference.invalidateRequest(new R66Result(
							new OpenR66ProtocolSystemException(
									"Not implemented"), session, true,
							ErrorCode.Unimplemented, null));
					final ErrorPacket errorPacket = new ErrorPacket(
							"Unimplemented Mesg: " +
									packet.getClass().getName(),
							ErrorCode.Unimplemented.getCode(),
							ErrorPacket.FORWARDCLOSECODE);
					ChannelUtils.writeAbstractLocalPacket(localChannelReference, errorPacket, true);
					ChannelUtils.close(e.getChannel());
					break;
				}
				case LocalPacketFactory.TESTPACKET: {
					session.newState(TEST);
					test(e.getChannel(), (TestPacket) packet);
					break;
				}
				case LocalPacketFactory.ENDTRANSFERPACKET: {
					endTransfer(e.getChannel(), (EndTransferPacket) packet);
					break;
				}
				case LocalPacketFactory.INFORMATIONPACKET: {
					session.newState(INFORMATION);
					information(e.getChannel(), (InformationPacket) packet);
					break;
				}
				case LocalPacketFactory.ENDREQUESTPACKET: {
					endRequest(e.getChannel(), (EndRequestPacket) packet);
					break;
				}
				case LocalPacketFactory.BUSINESSREQUESTPACKET: {
					businessRequest(e.getChannel(), (BusinessRequestPacket) packet);
					break;
				}
				case LocalPacketFactory.BLOCKREQUESTPACKET: {
					blockRequest(e.getChannel(), (BlockRequestPacket) packet);
					break;
				}
				default: {
					logger
							.error("Unknown Mesg: " +
									packet.getClass().getName());
					session.newState(ERROR);
					localChannelReference.invalidateRequest(new R66Result(
							new OpenR66ProtocolSystemException(
									"Unknown Message"), session, true,
							ErrorCode.Unimplemented, null));
					final ErrorPacket errorPacket = new ErrorPacket(
							"Unkown Mesg: " + packet.getClass().getName(),
							ErrorCode.Unimplemented.getCode(), ErrorPacket.FORWARDCLOSECODE);
					ChannelUtils.writeAbstractLocalPacket(localChannelReference, errorPacket, true);
					ChannelUtils.close(e.getChannel());
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.jboss.netty.channel.SimpleChannelHandler#exceptionCaught(org.jboss
	 * .netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ExceptionEvent)
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		// inform clients
		logger.debug("Exception and isFinished: "+
				(localChannelReference != null && localChannelReference.getFutureRequest().isDone()), e);
		if (localChannelReference != null && localChannelReference.getFutureRequest().isDone()) {
			Channels.close(e.getChannel());
			return;
		}
		OpenR66Exception exception = OpenR66ExceptionTrappedFactory
				.getExceptionFromTrappedException(e.getChannel(), e);
		ErrorCode code = null;
		if (exception != null) {
			session.newState(ERROR);
			boolean isAnswered = false;
			if (exception instanceof OpenR66ProtocolShutdownException) {
				logger.warn("Shutdown order received and going from: " +
						session.getAuth().getUser());
				if (localChannelReference != null) {
					R66Result finalValue = new R66Result(exception, session, true,
							ErrorCode.Shutdown, session.getRunner());
					try {
						tryFinalizeRequest(finalValue);
					} catch (OpenR66RunnerErrorException e2) {
					} catch (OpenR66ProtocolSystemException e2) {
					}
					if (!localChannelReference.getFutureRequest().isDone()) {
						try {
							session.setFinalizeTransfer(false, finalValue);
						} catch (OpenR66RunnerErrorException e1) {
							localChannelReference.invalidateRequest(finalValue);
						} catch (OpenR66ProtocolSystemException e1) {
							localChannelReference.invalidateRequest(finalValue);
						}
					}
				}
				// dont'close, thread will do
				ChannelUtils.startShutdown();
				// set global shutdown info and before close, send a valid
				// shutdown to all
				session.setStatus(54);
				return;
			} else {
				if (localChannelReference != null
						&& localChannelReference.getFutureRequest() != null) {
					if (localChannelReference.getFutureRequest().isDone()) {
						R66Result result = localChannelReference.getFutureRequest()
								.getResult();
						if (result != null) {
							isAnswered = result.isAnswered;
						}
					}
				}
				if (exception instanceof OpenR66ProtocolNoConnectionException) {
					code = ErrorCode.ConnectionImpossible;
					DbTaskRunner runner = session.getRunner();
					if (runner != null) {
						runner.stopOrCancelRunner(code);
					}
				} else if (exception instanceof OpenR66ProtocolBusinessCancelException) {
					code = ErrorCode.CanceledTransfer;
					DbTaskRunner runner = session.getRunner();
					if (runner != null) {
						runner.stopOrCancelRunner(code);
					}
				} else if (exception instanceof OpenR66ProtocolBusinessStopException) {
					code = ErrorCode.StoppedTransfer;
					DbTaskRunner runner = session.getRunner();
					if (runner != null) {
						runner.stopOrCancelRunner(code);
					}
				} else if (exception instanceof OpenR66ProtocolBusinessQueryAlreadyFinishedException) {
					code = ErrorCode.QueryAlreadyFinished;
					try {
						tryFinalizeRequest(new R66Result(session, true, code, session.getRunner()));
						return;
					} catch (OpenR66RunnerErrorException e1) {
					} catch (OpenR66ProtocolSystemException e1) {
					}
				} else if (exception instanceof OpenR66ProtocolBusinessQueryStillRunningException) {
					code = ErrorCode.QueryStillRunning;
					// nothing is to be done
					logger.error("Will close channel since ", exception);
					Channels.close(e.getChannel());
					session.setStatus(56);
					return;
				} else if (exception instanceof OpenR66ProtocolBusinessRemoteFileNotFoundException) {
					code = ErrorCode.FileNotFound;
				} else if (exception instanceof OpenR66RunnerException) {
					code = ErrorCode.ExternalOp;
				} else if (exception instanceof OpenR66ProtocolNotAuthenticatedException) {
					code = ErrorCode.BadAuthent;
				} else if (exception instanceof OpenR66ProtocolNetworkException) {
					code = ErrorCode.Disconnection;
					DbTaskRunner runner = session.getRunner();
					if (runner != null) {
						R66Result finalValue = new R66Result(
								new OpenR66ProtocolSystemException(
										"Finalize too early at close time with Network Exception"),
								session, true, code, session.getRunner());
						try {
							tryFinalizeRequest(finalValue);
						} catch (OpenR66Exception e2) {
						}
					}
				} else if (exception instanceof OpenR66ProtocolRemoteShutdownException) {
					code = ErrorCode.RemoteShutdown;
					DbTaskRunner runner = session.getRunner();
					if (runner != null) {
						runner.stopOrCancelRunner(code);
					}
				} else {
					DbTaskRunner runner = session.getRunner();
					if (runner != null) {
						switch (runner.getErrorInfo()) {
							case InitOk:
							case PostProcessingOk:
							case PreProcessingOk:
							case Running:
							case TransferOk:
								code = ErrorCode.Internal;
							default:
								code = runner.getErrorInfo();
						}
					} else {
						code = ErrorCode.Internal;
					}
				}
				if ((!isAnswered) &&
						(!(exception instanceof OpenR66ProtocolBusinessNoWriteBackException)) &&
						(!(exception instanceof OpenR66ProtocolNoConnectionException))) {
					if (code == null || code == ErrorCode.Internal) {
						code = ErrorCode.RemoteError;
					}
					final ErrorPacket errorPacket = new ErrorPacket(exception
							.getMessage(),
							code.getCode(), ErrorPacket.FORWARDCLOSECODE);
					try {
						ChannelUtils.writeAbstractLocalPacket(localChannelReference,
								errorPacket, true);
					} catch (OpenR66ProtocolPacketException e1) {
						// should not be
					}
				}
				R66Result finalValue =
						new R66Result(
								exception, session, true, code, session.getRunner());
				try {
					session.setFinalizeTransfer(false, finalValue);
					if (localChannelReference != null) {
						localChannelReference.invalidateRequest(finalValue);
					}
				} catch (OpenR66RunnerErrorException e1) {
					if (localChannelReference != null)
						localChannelReference.invalidateRequest(finalValue);
				} catch (OpenR66ProtocolSystemException e1) {
					if (localChannelReference != null)
						localChannelReference.invalidateRequest(finalValue);
				}
			}
			if (exception instanceof OpenR66ProtocolBusinessNoWriteBackException) {
				logger.error("Will close channel {}", exception.getMessage());
				Channels.close(e.getChannel());
				session.setStatus(56);
				return;
			} else if (exception instanceof OpenR66ProtocolNoConnectionException) {
				logger.error("Will close channel {}", exception.getMessage());
				Channels.close(e.getChannel());
				session.setStatus(57);
				return;
			}
			session.setStatus(58);
			ChannelCloseTimer.closeFutureChannel(e.getChannel());
		} else {
			// Nothing to do
			session.setStatus(59);
			return;
		}
	}

	/**
	 * Startup of the session and the local channel reference
	 * 
	 * @param channel
	 * @param packet
	 * @throws OpenR66ProtocolPacketException
	 */
	private void startup(Channel channel, StartupPacket packet)
			throws OpenR66ProtocolPacketException {
		localChannelReference = Configuration.configuration
				.getLocalTransaction().getFromId(packet.getLocalId());
		if (localChannelReference == null) {
			session.newState(ERROR);
			logger.error("Cannot startup");
			ErrorPacket error = new ErrorPacket("Cannot startup connection",
					ErrorCode.ConnectionImpossible.getCode(), ErrorPacket.FORWARDCLOSECODE);
			try {
				Channels.write(channel, error).await();
			} catch (InterruptedException e) {
			}
			// Cannot do writeBack(error, true);
			session.setStatus(40);
			ChannelCloseTimer.closeFutureChannel(channel);
			return;
		}
		NetworkChannel networkChannel =
				NetworkTransaction.getNetworkChannel(localChannelReference.getNetworkChannel());
		if (networkChannel != null) {
			localChannelReference.setNetworkChannelObject(networkChannel);
		} else {
			logger.error("No NetworkChannek found!");
		}
		session.newState(STARTUP);
		localChannelReference.validateStartup(true);
		session.setLocalChannelReference(localChannelReference);
		Channels.write(channel, packet);
		session.setStatus(41);
	}

	/**
	 * Refuse a connection
	 * 
	 * @param channel
	 * @param packet
	 * @param e1
	 * @throws OpenR66ProtocolPacketException
	 */
	private void refusedConnection(Channel channel, AuthentPacket packet, Exception e1)
			throws OpenR66ProtocolPacketException {
		logger.error("Cannot connect from "+
			localChannelReference.getNetworkChannel().getRemoteAddress()+
			" : " + packet.getHostId(), e1);
		if (Configuration.configuration.r66Mib != null) {
			Configuration.configuration.r66Mib.notifyError(
					"Connection not allowed from "+
					localChannelReference.getNetworkChannel().getRemoteAddress()
					+" since "+e1.getMessage(), packet.getHostId());
		}
		R66Result result = new R66Result(
				new OpenR66ProtocolSystemException(
						"Connection not allowed from "+
						localChannelReference.getNetworkChannel().getRemoteAddress(),
						e1), session, true,
				ErrorCode.BadAuthent, null);
		localChannelReference.invalidateRequest(result);
		session.newState(ERROR);
		ErrorPacket error = new ErrorPacket("Connection not allowed",
				ErrorCode.BadAuthent.getCode(),
				ErrorPacket.FORWARDCLOSECODE);
		ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
		localChannelReference.validateConnection(false, result);
		ChannelCloseTimer.closeFutureChannel(channel);
	}

	/**
	 * Authentication
	 * 
	 * @param channel
	 * @param packet
	 * @throws OpenR66ProtocolPacketException
	 */
	private void authent(Channel channel, AuthentPacket packet)
			throws OpenR66ProtocolPacketException {
		if (packet.isToValidate()) {
			session.newState(AUTHENTR);
		}

		if (localChannelReference.getDbSession() != null) {
			localChannelReference.getDbSession().useConnection();
		}
		try {
			session.getAuth().connection(localChannelReference.getDbSession(),
					packet.getHostId(), packet.getKey());
		} catch (Reply530Exception e1) {
			refusedConnection(channel, packet, e1);
			session.setStatus(42);
			return;
		} catch (Reply421Exception e1) {
			session.newState(ERROR);
			logger.error("Service unavailable: " + packet.getHostId(), e1);
			R66Result result = new R66Result(
					new OpenR66ProtocolSystemException("Service unavailable",
							e1), session, true,
					ErrorCode.ConnectionImpossible, null);
			localChannelReference.invalidateRequest(result);
			ErrorPacket error = new ErrorPacket("Service unavailable",
					ErrorCode.ConnectionImpossible.getCode(),
					ErrorPacket.FORWARDCLOSECODE);
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
			localChannelReference.validateConnection(false, result);
			ChannelCloseTimer.closeFutureChannel(channel);
			session.setStatus(43);
			return;
		}
		// Now if configuration say to do so: check remote ip address
		if (Configuration.configuration.checkRemoteAddress) {
			DbHostAuth host = R66Auth.getServerAuth(DbConstant.admin.session,
					packet.getHostId());
			boolean toTest = false;
			if (host.isClient()) {
				if (Configuration.configuration.checkClientAddress) {
					if (host.isNoAddress()) {
						// 0.0.0.0 so nothing
						toTest = false;
					} else {
						toTest = true;
					}
				}
			} else {
				toTest = true;
			}
			if (toTest) {
				// Real address so compare
				String address = host.getAddress();
				InetAddress[] inetAddress = null;
				try {
					inetAddress = InetAddress.getAllByName(address);
				} catch (UnknownHostException e) {
					inetAddress = null;
				}
				if (inetAddress != null) {
					InetSocketAddress socketAddress = (InetSocketAddress) session
							.getRemoteAddress();
					boolean found = false;
					for (int i = 0; i < inetAddress.length; i++) {
						if (socketAddress.getAddress().equals(inetAddress[i])) {
							found = true;
							break;
						}
					}
					if (!found) {
						// error
						refusedConnection(channel, packet,
								new OpenR66ProtocolNotAuthenticatedException(
										"Server IP not authenticated: " +
												inetAddress[0].toString() + " compare to "
												+ socketAddress.getAddress().toString()));
						session.setStatus(104);
						return;
					}
				}
			}
		}
		R66Result result = new R66Result(session, true, ErrorCode.InitOk, null);
		session.newState(AUTHENTD);
		localChannelReference.validateConnection(true, result);
		logger.debug("Local Server Channel Validated: {} ",
				(localChannelReference != null ? localChannelReference
						: "no LocalChannelReference"));
		session.setStatus(44);
		if (packet.isToValidate()) {
			// only requested
			NetworkTransaction.addClient(localChannelReference.getNetworkChannel(),
					packet.getHostId());
			packet.validate(session.getAuth().isSsl());
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, packet, false);
			session.setStatus(98);
		}
		logger.debug("versions: {}", Configuration.configuration.versions);
	}

	/**
	 * Receive a connection error
	 * 
	 * @param channel
	 * @param packet
	 */
	private void connectionError(Channel channel, ConnectionErrorPacket packet) {
		// do something according to the error
		logger.error(channel.getId() + ": " + packet.toString());
		ErrorCode code = ErrorCode.ConnectionImpossible;
		if (packet.getSmiddle() != null) {
			code = ErrorCode.getFromCode(packet.getSmiddle());
		}
		localChannelReference.invalidateRequest(new R66Result(
				new OpenR66ProtocolSystemException(packet.getSheader()),
				session, true, code, null));
		// True since closing
		session.newState(ERROR);
		session.setStatus(45);
		Channels.close(channel);
	}

	/**
	 * Class to finalize a runner when the future is over
	 * 
	 * @author Frederic Bregier
	 * 
	 */
	private class RunnerChannelFutureListener implements ChannelFutureListener {
		private LocalChannelReference localChannelReference;
		private R66Result result;

		public RunnerChannelFutureListener(LocalChannelReference localChannelReference,
				R66Result result) {
			this.localChannelReference = localChannelReference;
			this.result = result;
		}

		public void operationComplete(ChannelFuture future) throws Exception {
			localChannelReference.invalidateRequest(
					result);
			ChannelCloseTimer.closeFutureChannel(localChannelReference.getLocalChannel());
		}

	}

	/**
	 * Receive a remote error
	 * 
	 * @param channel
	 * @param packet
	 * @throws OpenR66RunnerErrorException
	 * @throws OpenR66ProtocolSystemException
	 * @throws OpenR66ProtocolBusinessException
	 */
	private void errorMesg(Channel channel, ErrorPacket packet)
			throws OpenR66RunnerErrorException, OpenR66ProtocolSystemException,
			OpenR66ProtocolBusinessException {
		// do something according to the error
		if (session.getLocalChannelReference().getFutureRequest().isDone()) {
			// already canceled or successful
			return;
		}
		logger.error(channel.getId() + ": " + packet.toString());
		session.setStatus(46);
		ErrorCode code = ErrorCode.getFromCode(packet.getSmiddle());
		session.getLocalChannelReference().setErrorMessage(packet.getSheader(), code);
		OpenR66ProtocolBusinessException exception;
		if (code.code == ErrorCode.CanceledTransfer.code) {
			exception =
					new OpenR66ProtocolBusinessCancelException(packet.getSheader());
			int rank = 0;
			DbTaskRunner runner = this.session.getRunner();
			if (runner != null) {
				runner.setRankAtStartup(rank);
				runner.stopOrCancelRunner(code);
			}
			R66Result result = new R66Result(exception, session,
					true, code, runner);
			// now try to inform other
			try {
				ChannelUtils.writeAbstractLocalPacket(localChannelReference, packet, false).
						addListener(new RunnerChannelFutureListener(localChannelReference, result));
			} catch (OpenR66ProtocolPacketException e) {
			}
			session.setFinalizeTransfer(false, result);
			return;
		} else if (code.code == ErrorCode.StoppedTransfer.code) {
			exception =
					new OpenR66ProtocolBusinessStopException(packet.getSheader());
			String[] vars = packet.getSheader().split(" ");
			String var = vars[vars.length - 1];
			int rank = Integer.parseInt(var);
			DbTaskRunner runner = this.session.getRunner();
			if (runner != null) {
				if (rank < runner.getRank()) {
					runner.setRankAtStartup(rank);
				}
				runner.stopOrCancelRunner(code);
			}
			R66Result result = new R66Result(exception, session,
					true, code, runner);
			// now try to inform other
			try {
				ChannelUtils.writeAbstractLocalPacket(localChannelReference, packet, false).
						addListener(new RunnerChannelFutureListener(localChannelReference, result));
			} catch (OpenR66ProtocolPacketException e) {
			}
			session.setFinalizeTransfer(false, result);
			return;
		} else if (code.code == ErrorCode.QueryAlreadyFinished.code) {
			DbTaskRunner runner = session.getRunner();
			if (runner == null) {
				exception =
						new OpenR66ProtocolBusinessCancelException(packet.toString());
			} else {
				if (runner.isSender()) {
					exception =
							new OpenR66ProtocolBusinessQueryAlreadyFinishedException(
									packet.getSheader());
					runner.finishTransferTask(code);
					tryFinalizeRequest(new R66Result(exception, session, true, code, runner));
				} else {
					exception =
							new OpenR66ProtocolBusinessCancelException(packet.toString());
				}
			}
			throw exception;
		} else if (code.code == ErrorCode.QueryStillRunning.code) {
			exception =
					new OpenR66ProtocolBusinessQueryStillRunningException(packet.getSheader());
			throw exception;
		} else if (code.code == ErrorCode.BadAuthent.code) {
			exception =
					new OpenR66ProtocolNotAuthenticatedException(packet.toString());
		} else if (code.code == ErrorCode.QueryRemotelyUnknown.code) {
			exception =
					new OpenR66ProtocolBusinessCancelException(packet.toString());
		} else if (code.code == ErrorCode.FileNotFound.code) {
			exception =
					new OpenR66ProtocolBusinessRemoteFileNotFoundException(packet.toString());
		} else {
			exception =
					new OpenR66ProtocolBusinessNoWriteBackException(packet.toString());
		}
		session.setFinalizeTransfer(false, new R66Result(exception, session,
				true, code, session.getRunner()));
		throw exception;
	}

	/**
	 * Finalize a request initialization in error
	 * 
	 * @param channel
	 * @param code
	 * @param runner
	 * @param e1
	 * @param packet
	 * @throws OpenR66ProtocolPacketException
	 */
	private void endInitRequestInError(Channel channel, ErrorCode code, DbTaskRunner runner,
			OpenR66Exception e1, RequestPacket packet) throws OpenR66ProtocolPacketException {
		logger.error("TaskRunner initialisation in error: " + code.mesg + " " + session
				+ " {} runner {}",
				e1 != null ? e1.getMessage() : "no exception",
				(runner != null ? runner.toShortString() : "no runner"));
		localChannelReference.invalidateRequest(new R66Result(
				e1, session, true, code, null));

		if (packet.isToValidate()) {
			// / answer with a wrong request since runner is not set on remote host
			if (runner != null) {
				if (runner.isSender()) {
					// In case Wildcard was used
					logger.debug("New FILENAME: {}", runner.getOriginalFilename());
					packet.setFilename(runner.getOriginalFilename());
					logger.debug("Rank set: " + runner.getRank());
					packet.setRank(runner.getRank());
				} else {
					logger.debug("Rank set: " + runner.getRank());
					packet.setRank(runner.getRank());
				}
			}
			packet.validate();
			packet.setCode(code.code);
			session.newState(ERROR);
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, packet, true);
		} else {
			session.newState(ERROR);
			ErrorPacket error = new ErrorPacket(
					"TaskRunner initialisation in error: " + e1
							.getMessage() + " for " + packet.toString() + " since " + code.mesg,
					code.getCode(), ErrorPacket.FORWARDCLOSECODE);
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
		}
		session.setStatus(47);
		ChannelCloseTimer.closeFutureChannel(channel);
	}

	/**
	 * Receive a request
	 * 
	 * @param channel
	 * @param packet
	 * @throws OpenR66ProtocolNoDataException
	 * @throws OpenR66ProtocolPacketException
	 * @throws OpenR66ProtocolBusinessException
	 * @throws OpenR66ProtocolSystemException
	 * @throws OpenR66RunnerErrorException
	 */
	private void request(Channel channel, RequestPacket packet)
			throws OpenR66ProtocolNoDataException, OpenR66ProtocolPacketException,
			OpenR66RunnerErrorException, OpenR66ProtocolSystemException,
			OpenR66ProtocolBusinessException {
		session.setStatus(99);
		if (!session.isAuthenticated()) {
			session.setStatus(48);
			throw new OpenR66ProtocolNotAuthenticatedException(
					"Not authenticated while Request received");
		}
		// XXX validLimit only on requested side
		if (packet.isToValidate()) {
			if (Configuration.configuration.isShutdown) {
				logger.info("Server is blocking new requests when receive request with Rule: "
						+ packet.getRulename() + " from " + session.getAuth().toString());
				session.setStatus(100);
				endInitRequestInError(channel,
						ErrorCode.ServerOverloaded, null,
						new OpenR66ProtocolNotYetConnectionException(
								"Limit exceeded"), packet);
				session.setStatus(100);
				return;
			}
			if (Configuration.configuration.constraintLimitHandler.checkConstraints()) {
				if (Configuration.configuration.r66Mib != null) {
					Configuration.configuration.r66Mib.
							notifyOverloaded("Rule: " + packet.getRulename() + " from "
									+ session.getAuth().toString(),
									Configuration.configuration.constraintLimitHandler.lastAlert);
				}
				logger.info("Limit exceeded when receive request with Rule: "
						+ packet.getRulename() + " from " + session.getAuth().toString());
				session.setStatus(100);
				endInitRequestInError(channel,
						ErrorCode.ServerOverloaded, null,
						new OpenR66ProtocolNotYetConnectionException(
								"Limit exceeded"), packet);
				session.setStatus(100);
				return;
			}
		} else if (packet.getCode() == ErrorCode.ServerOverloaded.code) {
			// XXX unvalid limit on requested host received
			logger.info("TaskRunner initialisation in error: " + ErrorCode.ServerOverloaded.mesg);
			localChannelReference.invalidateRequest(new R66Result(
					null, session, true, ErrorCode.ServerOverloaded, null));
			session.setStatus(101);
			ChannelCloseTimer.closeFutureChannel(channel);
			return;
		}
		DbRule rule;
		try {
			rule = new DbRule(localChannelReference.getDbSession(), packet.getRulename());
		} catch (WaarpDatabaseException e) {
			logger.info("Rule is unknown: " + packet.getRulename() + " {}", e.getMessage());
			session.setStatus(49);
			endInitRequestInError(channel,
					ErrorCode.QueryRemotelyUnknown, null,
					new OpenR66ProtocolBusinessException(
							"The Transfer is associated with an Unknown Rule: " +
									packet.getRulename()), packet);
			return;
		}
		int blocksize = packet.getBlocksize();
		if (packet.isToValidate()) {
			if (!rule.checkHostAllow(session.getAuth().getUser())) {
				session.setStatus(30);
				throw new OpenR66ProtocolNotAuthenticatedException(
						"Rule is not allowed for the remote host");
			}
			// Check if the blocksize is greater than local value
			if (Configuration.configuration.BLOCKSIZE < blocksize) {
				blocksize = Configuration.configuration.BLOCKSIZE;
				String sep = Configuration.getSeparator(session.getAuth().getUser());
				packet = new RequestPacket(packet.getRulename(), packet.getMode(),
						packet.getFilename(), blocksize, packet.getRank(),
						packet.getSpecialId(), packet.getFileInformation(), packet.getOriginalSize(), sep);
			}
		}
		if (!RequestPacket.isCompatibleMode(rule.mode, packet.getMode())) {
			// not compatible Rule and mode in request
			throw new OpenR66ProtocolNotAuthenticatedException(
					"Rule has not the same mode of transmission: " + rule.mode + " vs "
							+ packet.getMode());
		}
		session.setBlockSize(blocksize);
		DbTaskRunner runner;
		// requested
		boolean isRetrieve = DbTaskRunner.getSenderByRequestPacket(packet);
		if (packet.getSpecialId() != DbConstant.ILLEGALVALUE) {
			// Reload or create
			String requested = DbTaskRunner.getRequested(session, packet);
			String requester = DbTaskRunner.getRequester(session, packet);
			if (packet.isToValidate()) {
				// Id could be a creation or a reload
				// Try reload
				try {
					runner = new DbTaskRunner(localChannelReference.getDbSession(),
							session, rule, packet.getSpecialId(),
							requester, requested);
					runner.setSender(isRetrieve);
					if (runner.isAllDone()) {
						// truly an error since done
						session.setStatus(31);
						endInitRequestInError(channel,
								ErrorCode.QueryAlreadyFinished, runner,
								new OpenR66ProtocolBusinessQueryAlreadyFinishedException(
										"The TransferId is associated with a Transfer already finished: "
												+
												packet.getSpecialId()), packet);
						return;
					}
					LocalChannelReference lcr =
							Configuration.configuration.getLocalTransaction().
									getFromRequest(
											requested + " " + requester + " "
													+ packet.getSpecialId());
					if (lcr != null) {
						// truly an error since still running
						session.setStatus(32);
						endInitRequestInError(channel,
								ErrorCode.QueryStillRunning, runner,
								new OpenR66ProtocolBusinessQueryStillRunningException(
										"The TransferId is associated with a Transfer still running: "
												+
												packet.getSpecialId()), packet);
						return;
					}
					// ok to restart
					try {
						if (runner.restart(false)) {
							runner.saveStatus();
						}
					} catch (OpenR66RunnerErrorException e) {
					}
				} catch (WaarpDatabaseNoDataException e) {
					// Reception of request from requester host
					try {
						runner = new DbTaskRunner(localChannelReference.getDbSession(),
								session, rule, isRetrieve, packet);
					} catch (WaarpDatabaseException e1) {
						session.setStatus(33);
						endInitRequestInError(channel, ErrorCode.QueryRemotelyUnknown,
								null, new OpenR66DatabaseGlobalException(e), packet);
						return;
					}
				} catch (WaarpDatabaseException e) {
					session.setStatus(34);
					endInitRequestInError(channel, ErrorCode.QueryRemotelyUnknown, null,
							new OpenR66DatabaseGlobalException(e), packet);
					return;
				}
				// Change the SpecialID! => could generate an error ?
				packet.setSpecialId(runner.getSpecialId());
			} else {
				// Id should be a reload
				try {
					runner = new DbTaskRunner(localChannelReference.getDbSession(),
							session, rule, packet.getSpecialId(),
							requester, requested);
					runner.setSender(isRetrieve);
					// FIX check for SelfRequest
					if (runner.isSelfRequest()) {
						runner.setFilename(runner.getOriginalFilename());
					}
					if (! runner.isSender()) {
						logger.debug("New filename ? :" +packet.getFilename());
						runner.setOriginalFilename(packet.getFilename());
						runner.setFilename(packet.getFilename());
					}
					try {
						if (runner.restart(false)) {
							if (!runner.isSelfRequest()) {
								runner.saveStatus();
							}
						}
					} catch (OpenR66RunnerErrorException e) {
					}
				} catch (WaarpDatabaseException e) {
					if (localChannelReference.getDbSession() == null) {
						// Special case of no database client
						try {
							runner = new DbTaskRunner(localChannelReference.getDbSession(),
									session, rule, isRetrieve, packet);
						} catch (WaarpDatabaseException e1) {
							session.setStatus(35);
							endInitRequestInError(channel, ErrorCode.QueryRemotelyUnknown, null,
									new OpenR66DatabaseGlobalException(e1), packet);
							return;
						}
					} else {
						endInitRequestInError(channel, ErrorCode.QueryRemotelyUnknown, null,
								new OpenR66DatabaseGlobalException(e), packet);
						session.setStatus(36);
						return;
					}
				}
			}
		} else {
			// Very new request
			// should not be the case (the requester should always set the id)
			logger.error("NO TransferID specified: SHOULD NOT BE THE CASE");
			try {
				runner = new DbTaskRunner(localChannelReference.getDbSession(),
						session, rule, isRetrieve, packet);
			} catch (WaarpDatabaseException e) {
				session.setStatus(37);
				endInitRequestInError(channel, ErrorCode.QueryRemotelyUnknown, null,
						new OpenR66DatabaseGlobalException(e), packet);
				return;
			}
			packet.setSpecialId(runner.getSpecialId());
		}
		// Check now if request is a valid one
		if (packet.getCode() != ErrorCode.InitOk.code) {
			// not valid so create an error from there
			ErrorCode code = ErrorCode.getFromCode("" + packet.getCode());
			session.setBadRunner(runner, code);
			session.newState(ERROR);
			logger.error("Bad runner at startup {} {}", packet, session);
			ErrorPacket errorPacket = new ErrorPacket(code.mesg,
					code.getCode(), ErrorPacket.FORWARDCLOSECODE);
			errorMesg(channel, errorPacket);
			return;
		}
		// Receiver can specify a rank different from database
		if (runner.isSender()) {
			logger.debug("Rank was: " + runner.getRank() + " -> " + packet.getRank());
			runner.setRankAtStartup(packet.getRank());
		} else if (runner.getRank() > packet.getRank()) {
			logger.debug("Recv Rank was: " + runner.getRank() + " -> " + packet.getRank());
			// if receiver, change only if current rank is upper proposed rank
			runner.setRankAtStartup(packet.getRank());
		}
		boolean shouldInformBack = false;
		try {
			session.setRunner(runner);
			if (runner.isSender() && ! runner.isSendThrough()) {
				if (packet.getOriginalSize() != runner.getOriginalSize()) {
					packet.setOriginalSize(runner.getOriginalSize());
					shouldInformBack = true;
				}
			}
		} catch (OpenR66RunnerErrorException e) {
			try {
				runner.saveStatus();
			} catch (OpenR66RunnerErrorException e1) {
				logger.error("Cannot save Status: " + runner, e1);
			}
			if (runner.getErrorInfo() == ErrorCode.InitOk ||
					runner.getErrorInfo() == ErrorCode.PreProcessingOk ||
					runner.getErrorInfo() == ErrorCode.TransferOk) {
				runner.setErrorExecutionStatus(ErrorCode.ExternalOp);
			}
			logger.error("PreTask in error {}", e.getMessage());
			session.newState(ERROR);
			ErrorPacket error = new ErrorPacket("PreTask in error: " + e
					.getMessage(), runner.getErrorInfo().getCode(), ErrorPacket.FORWARDCLOSECODE);
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
			try {
				session.setFinalizeTransfer(false, new R66Result(e, session,
						true, runner.getErrorInfo(), runner));
			} catch (OpenR66RunnerErrorException e1) {
				localChannelReference.invalidateRequest(new R66Result(e, session,
						true, runner.getErrorInfo(), runner));
			} catch (OpenR66ProtocolSystemException e1) {
				localChannelReference.invalidateRequest(new R66Result(e, session,
						true, runner.getErrorInfo(), runner));
			}
			session.setStatus(38);
			ChannelCloseTimer.closeFutureChannel(channel);
			return;
		}
		if (packet.isToValidate()) {
			session.newState(REQUESTR);
		}
		if (runner.isFileMoved() && runner.isSender() && runner.isInTransfer()
				&& runner.getRank() == 0 && (!packet.isToValidate())) {
			// File was moved during PreTask and very beginning of the transfer
			// and the remote host has already received the request packet
			// => Informs the receiver of the new name
			logger.debug("Will send a modification of filename due to pretask: " +
					runner.getFilename());
			session.newState(VALID);
			ValidPacket validPacket = new ValidPacket("Change Filename by Pre action on sender",
					runner.getFilename()+Configuration.BAR_SEPARATOR_FIELD+packet.getOriginalSize(),
					LocalPacketFactory.REQUESTPACKET);
			ChannelUtils.writeAbstractLocalPacket(localChannelReference,
					validPacket, true);
		} else if ((!packet.getFilename().equals(runner.getOriginalFilename())) 
				&& runner.isSender() && runner.isInTransfer()
				&& runner.getRank() == 0 && (!packet.isToValidate())) {
			// File was modify at the very beginning (using wildcards)
			// and the remote host has already received the request packet
			// => Informs the receiver of the new name
			logger.debug("Will send a modification of filename due to wildcard: " +
					runner.getFilename());
			session.newState(VALID);
			ValidPacket validPacket = new ValidPacket("Change Filename by Wildcard on sender",
					runner.getFilename()+Configuration.BAR_SEPARATOR_FIELD+packet.getOriginalSize(), 
					LocalPacketFactory.REQUESTPACKET);
			ChannelUtils.writeAbstractLocalPacket(localChannelReference,
					validPacket, true);
		} else if (runner.isSelfRequest() && runner.isSender() && runner.isInTransfer()
				&& runner.getRank() == 0 && (!packet.isToValidate())) {
			// FIX SelfRequest
			// File could be modified at the very beginning (using wildcards)
			// and the remote host has already received the request packet
			// => Informs the receiver of the new name
			logger.debug("Will send a modification of filename due to wildcard: " +
					runner.getFilename());
			session.newState(VALID);
			ValidPacket validPacket = new ValidPacket("Change Filename by Wildcard on sender",
					runner.getFilename()+Configuration.BAR_SEPARATOR_FIELD+packet.getOriginalSize(), 
					LocalPacketFactory.REQUESTPACKET);
			ChannelUtils.writeAbstractLocalPacket(localChannelReference,
					validPacket, true);
		} else if (shouldInformBack) {
			// File length is now known, so inform back
			logger.debug("Will send a modification of filesize: " +
					runner.getOriginalSize());
			session.newState(VALID);
			ValidPacket validPacket = new ValidPacket("Change Filesize on sender",
					runner.getFilename()+Configuration.BAR_SEPARATOR_FIELD+packet.getOriginalSize(), 
					LocalPacketFactory.REQUESTPACKET);
			ChannelUtils.writeAbstractLocalPacket(localChannelReference,
					validPacket, true);
		}
		session.setReady(true);
		Configuration.configuration.getLocalTransaction().setFromId(runner, localChannelReference);
		// inform back
		if (packet.isToValidate()) {
			if (Configuration.configuration.monitoring != null) {
				Configuration.configuration.monitoring.lastInActiveTransfer =
						System.currentTimeMillis();
			}
			if (runner.isSender()) {
				// In case Wildcard was used
				logger.debug("New FILENAME: {}", runner.getOriginalFilename());
				packet.setFilename(runner.getOriginalFilename());
				logger.debug("Rank set: " + runner.getRank());
				packet.setRank(runner.getRank());
			} else {
				logger.debug("Rank set: " + runner.getRank());
				packet.setRank(runner.getRank());
			}
			packet.validate();
			session.newState(REQUESTD);
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, packet, true);
		} else {
			session.newState(REQUESTD);
			// requester => might be a client
			// Save the runner into the session and validate the request so begin transfer
			session.getLocalChannelReference().getFutureRequest().runner = runner;
			localChannelReference.getFutureValidRequest().setSuccess();
			if (Configuration.configuration.monitoring != null) {
				Configuration.configuration.monitoring.lastOutActiveTransfer =
						System.currentTimeMillis();
			}
		}
		// if retrieve => START the retrieve operation except if in Send Through mode
		if (runner.isSender()) {
			if (runner.isSendThrough()) {
				// it is legal to send data from now
				logger.debug("Now ready to continue with send through");
				localChannelReference.validateEndTransfer(
						new R66Result(session, false, ErrorCode.PreProcessingOk, runner));
			} else {
				// Automatically send data now
				logger.debug("Now ready to continue with runRetrieve");
				NetworkTransaction.runRetrieve(session, channel);
			}
		}
		session.setStatus(39);
	}

	/**
	 * Receive a data
	 * 
	 * @param channel
	 * @param packet
	 * @throws OpenR66ProtocolNotAuthenticatedException
	 * @throws OpenR66ProtocolBusinessException
	 * @throws OpenR66ProtocolPacketException
	 */
	private void data(Channel channel, DataPacket packet)
			throws OpenR66ProtocolNotAuthenticatedException,
			OpenR66ProtocolBusinessException, OpenR66ProtocolPacketException {
		if (!session.isAuthenticated()) {
			throw new OpenR66ProtocolNotAuthenticatedException(
					"Not authenticated while Data received");
		}
		if (!session.isReady()) {
			throw new OpenR66ProtocolBusinessException("No request prepared");
		}
		if (session.getRunner().isSender()) {
			throw new OpenR66ProtocolBusinessException(
					"Not in receive MODE but receive a packet");
		}
		if (!session.getRunner().continueTransfer()) {
			if (localChannelReference.getFutureEndTransfer().isFailed()) {
				// nothing to do since already done
				session.setStatus(94);
				return;
			}
			session.newState(ERROR);
			ErrorPacket error = new ErrorPacket(
					"Transfer in error due previously aborted transmission",
					ErrorCode.TransferError.getCode(), ErrorPacket.FORWARDCLOSECODE);
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
			try {
				session.setFinalizeTransfer(false, new R66Result(
						new OpenR66ProtocolPacketException(
								"Transfer was aborted previously"), session, true,
						ErrorCode.TransferError, session.getRunner()));
			} catch (OpenR66RunnerErrorException e1) {
			} catch (OpenR66ProtocolSystemException e1) {
			}
			session.setStatus(95);
			ChannelCloseTimer.closeFutureChannel(channel);
			return;
		}
		if (packet.getPacketRank() != session.getRunner().getRank()) {
			// Fix the rank if possible
			if (packet.getPacketRank() < session.getRunner().getRank()) {
				logger.debug("Bad RANK: " + packet.getPacketRank() + " : " +
						session.getRunner().getRank());
				session.getRunner().setRankAtStartup(packet.getPacketRank());
				session.getRestart().restartMarker(
						session.getRunner().getBlocksize() *
								session.getRunner().getRank());
				try {
					session.getFile().restartMarker(session.getRestart());
				} catch (CommandAbstractException e) {
					logger.error("Bad RANK: " + packet.getPacketRank() + " : " +
							session.getRunner().getRank());
					session.newState(ERROR);
					try {
						session.setFinalizeTransfer(false, new R66Result(
								new OpenR66ProtocolPacketException(
										"Bad Rank in transmission even after retry: " +
												packet.getPacketRank()), session, true,
								ErrorCode.TransferError, session.getRunner()));
					} catch (OpenR66RunnerErrorException e1) {
					} catch (OpenR66ProtocolSystemException e1) {
					}
					ErrorPacket error = new ErrorPacket(
							"Transfer in error due to bad rank transmission",
							ErrorCode.TransferError.getCode(), ErrorPacket.FORWARDCLOSECODE);
					ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
					session.setStatus(96);
					ChannelCloseTimer.closeFutureChannel(channel);
					return;
				}
			} else {
				// really bad
				logger.error("Bad RANK: " + packet.getPacketRank() + " : " +
						session.getRunner().getRank());
				session.newState(ERROR);
				try {
					session.setFinalizeTransfer(false, new R66Result(
							new OpenR66ProtocolPacketException(
									"Bad Rank in transmission: " +
											packet.getPacketRank() + " > " +
											session.getRunner().getRank()), session, true,
							ErrorCode.TransferError, session.getRunner()));
				} catch (OpenR66RunnerErrorException e1) {
				} catch (OpenR66ProtocolSystemException e1) {
				}
				ErrorPacket error = new ErrorPacket(
						"Transfer in error due to bad rank transmission",
						ErrorCode.TransferError.getCode(), ErrorPacket.FORWARDCLOSECODE);
				ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
				session.setStatus(20);
				ChannelCloseTimer.closeFutureChannel(channel);
				return;
			}
		}
		DataBlock dataBlock = new DataBlock();
		// if MD5 check MD5
		if (RequestPacket.isMD5Mode(session.getRunner().getMode())) {
			if (!packet.isKeyValid()) {
				// Wrong packet
				logger.error("Wrong MD5 Packet: {}", packet);
				session.newState(ERROR);
				try {
					session.setFinalizeTransfer(false, new R66Result(
							new OpenR66ProtocolPacketException(
									"Wrong Packet MD5"), session, true,
							ErrorCode.MD5Error, session.getRunner()));
				} catch (OpenR66RunnerErrorException e1) {
				} catch (OpenR66ProtocolSystemException e1) {
				}
				ErrorPacket error = new ErrorPacket(
						"Transfer in error due to bad MD5",
						ErrorCode.MD5Error.getCode(), ErrorPacket.FORWARDCLOSECODE);
				ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
				session.setStatus(21);
				ChannelCloseTimer.closeFutureChannel(channel);
				return;
			}
		}
		if (Configuration.configuration.globalDigest) {
			if (globalDigest == null) {
				try {
					// check if first block, since if not, digest will be only partial
					if (session.getRunner().getRank() > 0) {
						localChannelReference.setPartialHash();
					}
					globalDigest = new FilesystemBasedDigest(Configuration.configuration.digest);
				} catch (NoSuchAlgorithmException e) {
				}
			}
			FileUtils.computeGlobalHash(globalDigest, packet.getData());
		}
		if (session.getRunner().isRecvThrough() && localChannelReference.isRecvThroughMode()) {
			localChannelReference.getRecvThroughHandler().writeChannelBuffer(packet.getData());
			session.getRunner().incrementRank();
		} else {
			dataBlock.setBlock(packet.getData());
			try {
				session.getFile().writeDataBlock(dataBlock);
				session.getRunner().incrementRank();
			} catch (FileTransferException e) {
				session.newState(ERROR);
				try {
					session.setFinalizeTransfer(false, new R66Result(
							new OpenR66ProtocolSystemException(e), session, true,
							ErrorCode.TransferError, session.getRunner()));
				} catch (OpenR66RunnerErrorException e1) {
				} catch (OpenR66ProtocolSystemException e1) {
				}
				ErrorPacket error = new ErrorPacket("Transfer in error",
						ErrorCode.TransferError.getCode(), ErrorPacket.FORWARDCLOSECODE);
				ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
				session.setStatus(22);
				ChannelCloseTimer.closeFutureChannel(channel);
				return;
			}
		}
	}

	/**
	 * Test reception
	 * 
	 * @param channel
	 * @param packet
	 * @throws OpenR66ProtocolNotAuthenticatedException
	 * @throws OpenR66ProtocolPacketException
	 */
	private void test(Channel channel, TestPacket packet)
			throws OpenR66ProtocolNotAuthenticatedException,
			OpenR66ProtocolPacketException {
		if (!session.isAuthenticated()) {
			throw new OpenR66ProtocolNotAuthenticatedException(
					"Not authenticated while Test received");
		}
		// simply write back after+1
		packet.update();
		if (packet.getType() == LocalPacketFactory.VALIDPACKET) {
			ValidPacket validPacket = new ValidPacket(packet.toString(), null,
					LocalPacketFactory.TESTPACKET);
			R66Result result = new R66Result(session, true,
					ErrorCode.CompleteOk, null);
			result.other = validPacket;
			session.newState(VALIDOTHER);
			localChannelReference.validateRequest(result);
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, validPacket, true);
			logger.warn("Valid TEST MESSAGE from "+
					session.getAuth().getUser()+
					" ["+localChannelReference.getNetworkChannel().getRemoteAddress()+
					"] Msg=" +packet.toString());
			ChannelCloseTimer.closeFutureChannel(channel);
		} else {
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, packet, false);
		}
	}

	/**
	 * Receive an End of Transfer
	 * 
	 * @param channel
	 * @param packet
	 * @throws OpenR66RunnerErrorException
	 * @throws OpenR66ProtocolSystemException
	 * @throws OpenR66ProtocolNotAuthenticatedException
	 */
	private void endTransfer(Channel channel, EndTransferPacket packet)
			throws OpenR66RunnerErrorException, OpenR66ProtocolSystemException,
			OpenR66ProtocolNotAuthenticatedException {
		if (!session.isAuthenticated()) {
			throw new OpenR66ProtocolNotAuthenticatedException(
					"Not authenticated while EndTransfer received");
		}
		// Check end of transfer
		long originalSize = session.getRunner().getOriginalSize();
		logger.debug("OSize: "+originalSize+" isSender: "+session.getRunner().isSender());
		if (packet.isToValidate()) {
			// check if possible originalSize
			if (originalSize >= 0) {
				try {
					if (!session.getRunner().isRecvThrough() && session.getFile().length() != originalSize) {
						R66Result result = new R66Result(new OpenR66RunnerErrorException("Final size in error, transfer in error and rank should be reset to 0"),
								session, false, ErrorCode.TransferError, session.getRunner());
						localChannelReference.invalidateRequest(result);
						throw (OpenR66RunnerErrorException) result.exception;
					}
				} catch (CommandAbstractException e) {
					// ignore
				}
			}
			// check if possible Global Digest
			String hash = packet.getOptional();
			if (hash != null && globalDigest != null) {
				String localhash = FilesystemBasedDigest.getHex(globalDigest.Final());
				globalDigest = null;
				if (! localhash.equalsIgnoreCase(hash)) {
					// bad global Hash
					//session.getRunner().setRankAtStartup(0);
					R66Result result = new R66Result(new OpenR66RunnerErrorException("Global Hash in error, transfer in error and rank should be reset to 0"),
							session, false, ErrorCode.MD5Error, session.getRunner());
					localChannelReference.invalidateRequest(result);
					throw (OpenR66RunnerErrorException) result.exception;
				} else {
					localChannelReference.setHashComputeDuringTransfer(localhash);
					logger.debug("Global digest ok");
				}
			} else if (globalDigest != null) {
				String localhash = FilesystemBasedDigest.getHex(globalDigest.Final());
				globalDigest = null;
				localChannelReference.setHashComputeDuringTransfer(localhash);
			}
			globalDigest = null;
			session.newState(ENDTRANSFERS);
			if (!localChannelReference.getFutureRequest().isDone()) {
				// Finish with post Operation
				R66Result result = new R66Result(session, false,
						ErrorCode.TransferOk, session.getRunner());
				session.newState(ENDTRANSFERR);
				session.setFinalizeTransfer(true, result);
				// Now can send validation
				packet.validate();
				try {
					ChannelUtils.writeAbstractLocalPacket(localChannelReference,
							packet, false);
				} catch (OpenR66ProtocolPacketException e) {
					// ignore
				}
			} else {
				// in error due to a previous status (like bad MD5)
				logger
						.error("Error since end of transfer signaled but already done");
				session.setStatus(23);
				Channels.close(channel);
				return;
			}
		} else {
			session.newState(ENDTRANSFERR);
			if (!localChannelReference.getFutureRequest().isDone()) {
				// Validation of end of transfer
				R66Result result = new R66Result(session, false,
						ErrorCode.TransferOk, session.getRunner());
				session.setFinalizeTransfer(true, result);
			}
		}
	}

	/**
	 * Receive a request of information
	 * 
	 * @param channel
	 * @param packet
	 * @throws CommandAbstractException
	 * @throws OpenR66ProtocolNotAuthenticatedException
	 * @throws OpenR66ProtocolNoDataException
	 * @throws OpenR66ProtocolPacketException
	 */
	private void information(Channel channel, InformationPacket packet)
			throws OpenR66ProtocolNotAuthenticatedException,
			OpenR66ProtocolNoDataException, OpenR66ProtocolPacketException {
		if (!session.isAuthenticated()) {
			throw new OpenR66ProtocolNotAuthenticatedException(
					"Not authenticated while Information received");
		}
		byte request = packet.getRequest();
		if (request == -1) {
			// Id request
			String sid = packet.getRulename();
			String sisto = packet.getFilename();
			long id = DbConstant.ILLEGALVALUE;
			try {
				id = Long.parseLong(sid);
			} catch (NumberFormatException e) {
				logger.error("Incorrect Transfer ID", e);
				throw new OpenR66ProtocolNoDataException("Incorrect Transfer ID", e);
			}
			boolean isTo = sisto.equals("1");
			String remote = session.getAuth().getUser();
			String local = null;
			try {
				local = Configuration.configuration.getHostId(session.getAuth().isSsl());
			} catch (OpenR66ProtocolNoSslException e1) {
				logger.error("Local Ssl Host unknown is unknown", e1);
				throw new OpenR66ProtocolNoDataException("Local Ssl Host is unknown", e1);
			}
			DbTaskRunner runner = null;
			if (isTo) {
				try {
					runner = new DbTaskRunner(localChannelReference.getDbSession(), 
							localChannelReference.getSession(), null, 
							id, remote, local);
				} catch (WaarpDatabaseException e) {
					logger.error("RunnerTask is not found: " + packet.getRulename(), e);
					throw new OpenR66ProtocolNoDataException("Remote starting RunnerTask is not found: " + packet.getRulename(), e);
				}
			} else {
				try {
					runner = new DbTaskRunner(localChannelReference.getDbSession(),
							localChannelReference.getSession(), null, 
							id, local, remote);
				} catch (WaarpDatabaseException e) {
					logger.error("RunnerTask is not found: " + packet.getRulename() + ":" +id, e);
					throw new OpenR66ProtocolNoDataException("Local starting RunnerTask is not found: " + packet.getRulename(), e);
				}
			}
			session.newState(VALIDOTHER);
			ValidPacket validPacket;
			try {
				validPacket = new ValidPacket(runner.asXML(), "",
						LocalPacketFactory.INFORMATIONPACKET);
			} catch (OpenR66ProtocolBusinessException e) {
				logger.error("RunnerTask cannot be found: " + packet.getRulename(), e);
				throw new OpenR66ProtocolNoDataException("RunnerTask cannot be found: " + packet.getRulename(), e);
			}
			R66Result result = new R66Result(session, true,
					ErrorCode.CompleteOk, null);
			result.other = validPacket;
			localChannelReference.validateEndTransfer(result);
			localChannelReference.validateRequest(result);
			ChannelUtils.writeAbstractLocalPacket(localChannelReference,
					validPacket, true);
			Channels.close(channel);
			return;
		}
		DbRule rule;
		try {
			rule = new DbRule(localChannelReference.getDbSession(), packet.getRulename());
		} catch (WaarpDatabaseException e) {
			logger.error("Rule is unknown: " + packet.getRulename(), e);
			throw new OpenR66ProtocolNoDataException(e);
		}
		try {
			if (RequestPacket.isRecvMode(rule.mode)) {
				session.getDir().changeDirectory(rule.workPath);
			} else {
				session.getDir().changeDirectory(rule.sendPath);
			}

			if (request == InformationPacket.ASKENUM.ASKLIST.ordinal() ||
					request == InformationPacket.ASKENUM.ASKMLSLIST.ordinal()) {
				// ls or mls from current directory
				List<String> list;
				if (request == InformationPacket.ASKENUM.ASKLIST.ordinal()) {
					list = session.getDir().list(packet.getFilename());
				} else {
					list = session.getDir().listFull(packet.getFilename(), false);
				}

				StringBuilder builder = new StringBuilder();
				for (String elt : list) {
					builder.append(elt);
					builder.append('\n');
				}
				session.newState(VALIDOTHER);
				ValidPacket validPacket = new ValidPacket(builder.toString(), "" + list.size(),
						LocalPacketFactory.INFORMATIONPACKET);
				R66Result result = new R66Result(session, true,
						ErrorCode.CompleteOk, null);
				result.other = validPacket;
				localChannelReference.validateEndTransfer(result);
				localChannelReference.validateRequest(result);
				ChannelUtils.writeAbstractLocalPacket(localChannelReference,
						validPacket, true);
				Channels.close(channel);
			} else {
				// ls pr mls from current directory and filename
				R66File file = (R66File) session.getDir().setFile(packet.getFilename(), false);
				String sresult = null;
				if (request == InformationPacket.ASKENUM.ASKEXIST.ordinal()) {
					sresult = "" + file.exists();
				} else if (request == InformationPacket.ASKENUM.ASKMLSDETAIL.ordinal()) {
					sresult = session.getDir().fileFull(packet.getFilename(), false);
					String[] list = sresult.split("\n");
					sresult = list[1];
				} else {
					session.newState(ERROR);
					ErrorPacket error = new ErrorPacket("Unknown Request " + request,
							ErrorCode.Warning.getCode(), ErrorPacket.FORWARDCLOSECODE);
					ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
					ChannelCloseTimer.closeFutureChannel(channel);
					return;
				}
				session.newState(VALIDOTHER);
				ValidPacket validPacket = new ValidPacket(sresult, "1",
						LocalPacketFactory.INFORMATIONPACKET);
				R66Result result = new R66Result(session, true,
						ErrorCode.CompleteOk, null);
				result.other = validPacket;
				localChannelReference.validateEndTransfer(result);
				localChannelReference.validateRequest(result);
				ChannelUtils.writeAbstractLocalPacket(localChannelReference,
						validPacket, true);
				ChannelCloseTimer.closeFutureChannel(channel);
			}
		} catch (CommandAbstractException e) {
			session.newState(ERROR);
			ErrorPacket error = new ErrorPacket("Error while Request " + request + " "
					+ e.getMessage(),
					ErrorCode.Internal.getCode(), ErrorPacket.FORWARDCLOSECODE);
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
			ChannelCloseTimer.closeFutureChannel(channel);
		}
	}

	/**
	 * Stop or Cancel a Runner
	 * 
	 * @param id
	 * @param reqd
	 * @param reqr
	 * @param code
	 * @return True if correctly stopped or canceled
	 */
	private boolean stopOrCancelRunner(long id, String reqd, String reqr, ErrorCode code) {
		try {
			DbTaskRunner taskRunner =
					new DbTaskRunner(localChannelReference.getDbSession(), session,
							null, id, reqr, reqd);
			return taskRunner.stopOrCancelRunner(code);
		} catch (WaarpDatabaseException e) {
		}
		return false;
	}

	/**
	 * Receive a validation or a special request
	 * 
	 * @param channel
	 * @param packet
	 * @throws OpenR66ProtocolNotAuthenticatedException
	 * @throws OpenR66RunnerErrorException
	 * @throws OpenR66ProtocolSystemException
	 * @throws OpenR66ProtocolBusinessException
	 */
	private void valid(Channel channel, ValidPacket packet)
			throws OpenR66ProtocolNotAuthenticatedException,
			OpenR66RunnerErrorException, OpenR66ProtocolSystemException,
			OpenR66ProtocolBusinessException {
		if (packet.getTypeValid() != LocalPacketFactory.SHUTDOWNPACKET &&
				(!session.isAuthenticated())) {
			logger.warn("Valid packet received while not authenticated: {} {}", packet, session);
			session.newState(ERROR);
			throw new OpenR66ProtocolNotAuthenticatedException(
					"Not authenticated while Valid received");
		}
		switch (packet.getTypeValid()) {
			case LocalPacketFactory.SHUTDOWNPACKET: {
				session.newState(SHUTDOWN);
				logger.warn("Shutdown received so Will close channel" +
						localChannelReference.toString());
				R66Result result = new R66Result(
						new OpenR66ProtocolShutdownException(), session, true,
						ErrorCode.Shutdown, session.getRunner());
				result.other = packet;
				if (session.getRunner() != null &&
						session.getRunner().isInTransfer()) {
					String srank = packet.getSmiddle();
					DbTaskRunner runner = session.getRunner();
					if (srank != null && srank.length() > 0) {
						// Save last rank from remote point of view
						try {
							int rank = Integer.parseInt(srank);
							runner.setRankAtStartup(rank);
						} catch (NumberFormatException e) {
							// ignore
						}
						session.setFinalizeTransfer(false, result);
					} else if (!runner.isSender()) {
						// is receiver so informs back for the rank to use next time
						int newrank = runner.getRank();
						packet.setSmiddle(Integer.toString(newrank));
						try {
							runner.saveStatus();
						} catch (OpenR66RunnerErrorException e) {
						}
						session.setFinalizeTransfer(false, result);
						try {
							ChannelUtils.writeAbstractLocalPacket(localChannelReference, packet,
									true);
						} catch (OpenR66ProtocolPacketException e) {
						}
					} else {
						session.setFinalizeTransfer(false, result);
					}
				} else {
					session.setFinalizeTransfer(false, result);
				}
				session.setStatus(26);
				try {
					Thread.sleep(Configuration.WAITFORNETOP * 2);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				logger.warn("Will Close Local from Network Channel");
				Configuration.configuration.getLocalTransaction()
						.closeLocalChannelsFromNetworkChannel(localChannelReference
								.getNetworkChannel());
				NetworkTransaction
						.shuttingdownNetworkChannel(localChannelReference
								.getNetworkChannel());
				ChannelCloseTimer.closeFutureChannel(channel);
				break;
			}
			case LocalPacketFactory.STOPPACKET:
			case LocalPacketFactory.CANCELPACKET: {
				session.newState(VALIDOTHER);
				// Authentication must be the local server
				try {
					if (!session.getAuth().getUser().equals(
							Configuration.configuration.getHostId(session.getAuth().isSsl()))) {
						throw new OpenR66ProtocolNotAuthenticatedException(
								"Not correctly authenticated");
					}
				} catch (OpenR66ProtocolNoSslException e1) {
					throw new OpenR66ProtocolNotAuthenticatedException(
							"Not correctly authenticated since SSL is not supported", e1);
				}
				// header = ?; middle = requested+blank+requester+blank+specialId
				LocalChannelReference lcr =
						Configuration.configuration.getLocalTransaction().
								getFromRequest(packet.getSmiddle());
				// stop the current transfer
				R66Result resulttest;
				ErrorCode code = (packet.getTypeValid() == LocalPacketFactory.STOPPACKET) ?
						ErrorCode.StoppedTransfer : ErrorCode.CanceledTransfer;
				if (lcr != null) {
					int rank = 0;
					if (code == ErrorCode.StoppedTransfer && lcr.getSession() != null) {
						DbTaskRunner taskRunner = lcr.getSession().getRunner();
						if (taskRunner != null) {
							rank = taskRunner.getRank();
						}
					}
					session.newState(ERROR);
					ErrorPacket error = new ErrorPacket(code.name() + " " + rank,
							code.getCode(), ErrorPacket.FORWARDCLOSECODE);
					try {
						// XXX ChannelUtils.writeAbstractLocalPacket(lcr, error);
						// inform local instead of remote
						ChannelUtils.writeAbstractLocalPacketToLocal(lcr, error);
					} catch (Exception e) {
					}
					resulttest = new R66Result(session, true,
							ErrorCode.CompleteOk, session.getRunner());
				} else {
					// Transfer is not running
					// but maybe need action on database
					String[] keys = packet.getSmiddle().split(" ");
					long id = Long.parseLong(keys[2]);
					if (stopOrCancelRunner(id, keys[0], keys[1], code)) {
						resulttest = new R66Result(session, true,
								ErrorCode.CompleteOk, session.getRunner());
					} else {
						resulttest = new R66Result(session, true,
								ErrorCode.TransferOk, session.getRunner());
					}
				}
				// inform back the requester
				ValidPacket valid = new ValidPacket(packet.getSmiddle(), resulttest.code.getCode(),
						LocalPacketFactory.REQUESTUSERPACKET);
				resulttest.other = packet;
				localChannelReference.validateRequest(resulttest);
				try {
					ChannelUtils.writeAbstractLocalPacket(localChannelReference,
							valid, true);
				} catch (OpenR66ProtocolPacketException e) {
				}
				session.setStatus(27);
				Channels.close(channel);
				break;
			}
			case LocalPacketFactory.VALIDPACKET: {
				session.newState(VALIDOTHER);
				// Try to validate a restarting transfer
				// XXX validLimit on requested side
				if (Configuration.configuration.constraintLimitHandler.checkConstraints()) {
					logger.error("Limit exceeded while asking to relaunch a task"
							+ packet.getSmiddle());
					session.setStatus(100);
					ValidPacket valid;
					valid = new ValidPacket(packet.getSmiddle(),
							ErrorCode.ServerOverloaded.getCode(),
							LocalPacketFactory.REQUESTUSERPACKET);
					R66Result resulttest = new R66Result(null, session, true,
							ErrorCode.Internal, null);
					resulttest.other = packet;
					localChannelReference.invalidateRequest(resulttest);
					// inform back the requester
					try {
						ChannelUtils.writeAbstractLocalPacket(localChannelReference,
								valid, true);
					} catch (OpenR66ProtocolPacketException e) {
					}
					Channels.close(channel);
					return;
				}
				// Try to validate a restarting transfer
				// header = ?; middle = requested+blank+requester+blank+specialId
				// note: might contains one more argument = time to reschedule in yyyyMMddHHmmss format
				String[] keys = packet.getSmiddle().split(" ");
				ValidPacket valid;
				if (keys.length < 3) {
					// not enough args
					valid = new ValidPacket(packet.getSmiddle(),
							ErrorCode.Internal.getCode(),
							LocalPacketFactory.REQUESTUSERPACKET);
					R66Result resulttest = new R66Result(
							new OpenR66ProtocolBusinessRemoteFileNotFoundException("Not enough arguments"),
							session, true,
							ErrorCode.Internal, null);
					resulttest.other = packet;
					localChannelReference.invalidateRequest(resulttest);
				} else {
					long id = Long.parseLong(keys[2]);
					DbTaskRunner taskRunner = null;
					try {
						taskRunner = new DbTaskRunner(localChannelReference.getDbSession(), session,
								null, id, keys[1], keys[0]);
						Timestamp timestart = null;
						if (keys.length > 3) {
							// time to reschedule in yyyyMMddHHmmss format
							logger.debug("Debug: restart with "+keys[3]);
							SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
							try {
								Date date = dateFormat.parse(keys[3]);
								timestart = new Timestamp(date.getTime());
								taskRunner.setStart(timestart);
							} catch (ParseException e) {
							}
						}
						LocalChannelReference lcr =
								Configuration.configuration.getLocalTransaction().
										getFromRequest(packet.getSmiddle());
						// since it comes from a request transfer, cannot redo it
						logger.info("Will try to restart: "+taskRunner.toShortString());
						R66Result resulttest = TransferUtils.restartTransfer(taskRunner, lcr);
						valid = new ValidPacket(packet.getSmiddle(), resulttest.code.getCode(),
								LocalPacketFactory.REQUESTUSERPACKET);
						resulttest.other = packet;
						localChannelReference.validateRequest(resulttest);
					} catch (WaarpDatabaseException e1) {
						valid = new ValidPacket(packet.getSmiddle(),
								ErrorCode.Internal.getCode(),
								LocalPacketFactory.REQUESTUSERPACKET);
						R66Result resulttest = new R66Result(new OpenR66DatabaseGlobalException(e1),
								session, true,
								ErrorCode.Internal, taskRunner);
						resulttest.other = packet;
						localChannelReference.invalidateRequest(resulttest);
					}
				}
				// inform back the requester
				try {
					ChannelUtils.writeAbstractLocalPacket(localChannelReference,
							valid, true);
				} catch (OpenR66ProtocolPacketException e) {
				}
				Channels.close(channel);
				break;
			}
			case LocalPacketFactory.REQUESTUSERPACKET: {
				session.newState(VALIDOTHER);
				// Validate user request
				R66Result resulttest = new R66Result(session, true,
						ErrorCode.getFromCode(packet.getSmiddle()), null);
				resulttest.other = packet;
				switch (resulttest.code) {
					case CompleteOk:
					case InitOk:
					case PostProcessingOk:
					case PreProcessingOk:
					case QueryAlreadyFinished:
					case QueryStillRunning:
					case Running:
					case TransferOk:
						break;
					default:
						localChannelReference.invalidateRequest(resulttest);
						session.setStatus(102);
						Channels.close(channel);
						return;
				}
				localChannelReference.validateRequest(resulttest);
				session.setStatus(28);
				Channels.close(channel);
				break;
			}
			case LocalPacketFactory.LOGPACKET:
			case LocalPacketFactory.LOGPURGEPACKET: {
				session.newState(VALIDOTHER);
				// should be from the local server or from an authorized hosts: LOGCONTROL
				if (!session.getAuth().isValidRole(ROLE.LOGCONTROL)) {
					throw new OpenR66ProtocolNotAuthenticatedException(
							"Not correctly authenticated");
				}
				String sstart = packet.getSheader();
				String sstop = packet.getSmiddle();
				boolean isPurge = (packet.getTypeValid() == LocalPacketFactory.LOGPURGEPACKET) ?
						true : false;
				Timestamp start = (sstart == null || sstart.length() == 0) ? null :
						Timestamp.valueOf(sstart);
				Timestamp stop = (sstop == null || sstop.length() == 0) ? null :
						Timestamp.valueOf(sstop);
				// create export of log and optionally purge them from database
				DbPreparedStatement getValid = null;
				String filename = Configuration.configuration.baseDirectory +
						Configuration.configuration.archivePath + R66Dir.SEPARATOR +
						Configuration.configuration.HOST_ID + "_" + System.currentTimeMillis() +
						"_runners.xml";
				try {
					getValid =
							DbTaskRunner.getLogPrepareStatement(
									localChannelReference.getDbSession(),
									start, stop);
					DbTaskRunner.writeXMLWriter(getValid, filename);
				} catch (WaarpDatabaseNoConnectionException e1) {
					throw new OpenR66ProtocolBusinessException(e1);
				} catch (WaarpDatabaseSqlException e1) {
					throw new OpenR66ProtocolBusinessException(e1);
				} finally {
					if (getValid != null) {
						getValid.realClose();
					}
				}
				// in case of purge
				int nb = 0;
				if (isPurge) {
					// purge in same interval all runners with globallaststep
					// as ALLDONETASK or ERRORTASK
					if (Configuration.configuration.r66Mib != null) {
						Configuration.configuration.r66Mib.notifyWarning(
								"Purge Log Order received", session.getAuth().getUser());
					}
					try {
						nb = DbTaskRunner.purgeLogPrepareStatement(
								localChannelReference.getDbSession(),
								start, stop);
					} catch (WaarpDatabaseNoConnectionException e) {
						throw new OpenR66ProtocolBusinessException(e);
					} catch (WaarpDatabaseSqlException e) {
						throw new OpenR66ProtocolBusinessException(e);
					}
				}
				R66Result result = new R66Result(session, true, ErrorCode.CompleteOk, null);
				// Now answer
				ValidPacket valid = new ValidPacket(filename + " " + nb, result.code.getCode(),
						LocalPacketFactory.REQUESTUSERPACKET);
				localChannelReference.validateRequest(result);
				try {
					ChannelUtils.writeAbstractLocalPacket(localChannelReference,
							valid, true);
				} catch (OpenR66ProtocolPacketException e) {
				}
				Channels.close(channel);
				break;
			}
			case LocalPacketFactory.CONFEXPORTPACKET: {
				session.newState(VALIDOTHER);
				if (Configuration.configuration.r66Mib != null) {
					Configuration.configuration.r66Mib.notifyWarning(
							"Export Configuration Order received", session.getAuth().getUser());
				}
				String shost = packet.getSheader();
				String srule = packet.getSmiddle();
				boolean bhost = Boolean.parseBoolean(shost);
				boolean brule = Boolean.parseBoolean(srule);
				String dir = Configuration.configuration.baseDirectory +
						Configuration.configuration.archivePath;
				String hostname = Configuration.configuration.HOST_ID;
				if (bhost) {
					String filename = dir + File.separator + hostname + "_Authentications.xml";
					try {
						AuthenticationFileBasedConfiguration.writeXML(Configuration.configuration,
								filename);
						shost = filename;
					} catch (WaarpDatabaseNoConnectionException e) {
						logger.error("Error", e);
						shost = "#";
						bhost = false;
					} catch (WaarpDatabaseSqlException e) {
						logger.error("Error", e);
						shost = "#";
						bhost = false;
					} catch (OpenR66ProtocolSystemException e) {
						logger.error("Error", e);
						shost = "#";
						bhost = false;
					}
				}
				if (brule) {
					try {
						srule = RuleFileBasedConfiguration.writeOneXml(dir, hostname);
					} catch (WaarpDatabaseNoConnectionException e1) {
						logger.error("Error", e1);
						srule = "#";
						brule = false;
					} catch (WaarpDatabaseSqlException e1) {
						logger.error("Error", e1);
						srule = "#";
						brule = false;
					} catch (OpenR66ProtocolSystemException e1) {
						logger.error("Error", e1);
						srule = "#";
						brule = false;
					}
				}
				R66Result result = null;
				if (brule || bhost) {
					result = new R66Result(session, true, ErrorCode.CompleteOk, null);
				} else {
					result = new R66Result(session, true, ErrorCode.TransferError, null);
				}
				// Now answer
				ValidPacket valid = new ValidPacket(shost + " " + srule, result.code.getCode(),
						LocalPacketFactory.REQUESTUSERPACKET);
				localChannelReference.validateRequest(result);
				try {
					ChannelUtils.writeAbstractLocalPacket(localChannelReference,
							valid, true);
				} catch (OpenR66ProtocolPacketException e) {
				}
				Channels.close(channel);
				break;
			}
			case LocalPacketFactory.CONFIMPORTPACKET: {
				session.newState(VALIDOTHER);
				if (Configuration.configuration.r66Mib != null) {
					Configuration.configuration.r66Mib.notifyWarning(
							"Import Configuration Order received", session.getAuth().getUser());
				}
				String shost = packet.getSheader();
				String srule = packet.getSmiddle();
				boolean bhostPurge = shost.startsWith("1 ");
				shost = shost.substring(2);
				boolean brulePurge = srule.startsWith("1 ");
				srule = srule.substring(2);
				boolean bhost = shost.length() > 0;
				boolean brule = srule.length() > 0;
				if (bhost) {
					DbHostAuth[] oldHosts = null;
					if (bhostPurge) {
						// Need to first delete all entries
						try {
							oldHosts = DbHostAuth.deleteAll(DbConstant.admin.session);
						} catch (WaarpDatabaseException e) {
							// ignore
						}
					}
					String filename = shost;
					if (AuthenticationFileBasedConfiguration.loadAuthentication(
							Configuration.configuration,
							filename)) {
						shost = "Host:OK";
					} else {
						logger.error("Error in Load Hosts");
						shost = "Host:KO";
						bhost = false;
					}
					if (!bhost) {
						if (oldHosts != null) {
							for (DbHostAuth dbHost : oldHosts) {
								try {
									if (!dbHost.exist()) {
										dbHost.insert();
									}
								} catch (WaarpDatabaseException e1) {
									// ignore
								}
							}
						}
					}
				}
				if (brule) {
					DbRule[] oldRules = null;
					if (brulePurge) {
						// Need to first delete all entries
						try {
							oldRules = DbRule.deleteAll(DbConstant.admin.session);
						} catch (WaarpDatabaseException e) {
							// ignore
						}
					}
					File file = new File(srule);
					try {
						RuleFileBasedConfiguration.getMultipleFromFile(file);
						srule = "Rule:OK";
						brule = true;
					} catch (WaarpDatabaseNoConnectionException e) {
						logger.error("Error", e);
						srule = "Rule:KO";
						brule = false;
					} catch (WaarpDatabaseSqlException e) {
						logger.error("Error", e);
						srule = "Rule:KO";
						brule = false;
					} catch (WaarpDatabaseNoDataException e) {
						logger.error("Error", e);
						srule = "Rule:KO";
						brule = false;
					} catch (WaarpDatabaseException e) {
						logger.error("Error", e);
						srule = "Rule:KO";
						brule = false;
					}
					if (!brule) {
						if (oldRules != null) {
							for (DbRule dbRule : oldRules) {
								try {
									if (!dbRule.exist()) {
										dbRule.insert();
									}
								} catch (WaarpDatabaseException e1) {
									// ignore
								}
							}
						}
					}
				}
				R66Result result = null;
				if (brule || bhost) {
					result = new R66Result(session, true, ErrorCode.CompleteOk, null);
				} else {
					result = new R66Result(session, true, ErrorCode.TransferError, null);
				}
				// Now answer
				ValidPacket valid = new ValidPacket(shost + " " + srule, result.code.getCode(),
						LocalPacketFactory.REQUESTUSERPACKET);
				localChannelReference.validateRequest(result);
				try {
					ChannelUtils.writeAbstractLocalPacket(localChannelReference,
							valid, true);
				} catch (OpenR66ProtocolPacketException e) {
				}
				Channels.close(channel);
				break;
			}
			case LocalPacketFactory.INFORMATIONPACKET: {
				session.newState(VALIDOTHER);
				// Validate user request
				R66Result resulttest = new R66Result(session, true,
						ErrorCode.CompleteOk, null);
				resulttest.other = packet;
				localChannelReference.validateRequest(resulttest);
				Channels.close(channel);
				break;
			}
			case LocalPacketFactory.REQUESTPACKET: {
				session.newState(VALID);
				// The filename or filesize from sender is changed due to PreTask so change it too in receiver
				String [] fields = packet.getSmiddle().split(Configuration.BAR_SEPARATOR_FIELD);
				String newfilename = fields[0];
				// potential file size changed
				long newSize = -1;
				if (fields.length > 1) {
					try {
						newSize = Long.parseLong(fields[fields.length-1]);
						if (session.getRunner() != null) {
							session.getRunner().setOriginalSize(newSize);
						}
					} catch (NumberFormatException e) {
						newfilename += Configuration.BAR_SEPARATOR_FIELD + fields[fields.length-1];
					}
				}
				// check if send is already on going
				if (session.getRunner() != null && session.getRunner().getRank() > 0) {
					// already started so not changing the filename
					// Success: No write back at all
					break;
				}
				// Pre execution was already done since this packet is only received once
				// the request is already validated by the receiver
				try {
					session.renameReceiverFile(newfilename);
				} catch (OpenR66RunnerErrorException e) {
					DbTaskRunner runner = session.getRunner();
					runner.saveStatus();
					runner.setErrorExecutionStatus(ErrorCode.FileNotFound);
					session.newState(ERROR);
					logger.error("File renaming in error {}", e.getMessage());
					ErrorPacket error = new ErrorPacket("File renaming in error: " + e
							.getMessage(), runner.getErrorInfo().getCode(),
							ErrorPacket.FORWARDCLOSECODE);
					try {
						ChannelUtils.writeAbstractLocalPacket(localChannelReference,
								error, true);
					} catch (OpenR66ProtocolPacketException e2) {
					}
					try {
						session.setFinalizeTransfer(false, new R66Result(e, session,
								true, runner.getErrorInfo(), runner));
					} catch (OpenR66RunnerErrorException e1) {
						localChannelReference.invalidateRequest(new R66Result(e, session,
								true, runner.getErrorInfo(), runner));
					} catch (OpenR66ProtocolSystemException e1) {
						localChannelReference.invalidateRequest(new R66Result(e, session,
								true, runner.getErrorInfo(), runner));
					}
					session.setStatus(97);
					ChannelCloseTimer.closeFutureChannel(channel);
					return;
				}
				// Success: No write back at all
				break;
			}
			case LocalPacketFactory.BANDWIDTHPACKET: {
				session.newState(VALIDOTHER);
				// should be from the local server or from an authorized hosts: LIMIT
				if (!session.getAuth().isValidRole(ROLE.LIMIT)) {
					throw new OpenR66ProtocolNotAuthenticatedException(
							"Not correctly authenticated");
				}
				String[] splitglobal = packet.getSheader().split(" ");
				String[] splitsession = packet.getSmiddle().split(" ");
				if (splitglobal.length < 2 || splitsession.length < 2) {
					// request of current values
					R66Result result = new R66Result(session, true, ErrorCode.CompleteOk, null);
					// Now answer
					ValidPacket valid = new ValidPacket(Configuration.configuration.serverGlobalWriteLimit
							+" "+Configuration.configuration.serverGlobalReadLimit+
							" "+Configuration.configuration.serverChannelWriteLimit+
							" "+Configuration.configuration.serverChannelReadLimit, result.code.getCode(),
							LocalPacketFactory.REQUESTUSERPACKET);
					localChannelReference.validateRequest(result);
					try {
						ChannelUtils.writeAbstractLocalPacket(localChannelReference,
								valid, true);
					} catch (OpenR66ProtocolPacketException e) {
					}
					Channels.close(channel);
					return;
				}
				long wgl = (Long.parseLong(splitglobal[0]) / 10) * 10;
				long rgl = (Long.parseLong(splitglobal[1]) / 10) * 10;
				long wsl = (Long.parseLong(splitsession[0]) / 10) * 10;
				long rsl = (Long.parseLong(splitsession[1]) / 10) * 10;
				if (wgl < 0) {
					wgl = Configuration.configuration.serverGlobalWriteLimit;
				}
				if (rgl < 0) {
					rgl = Configuration.configuration.serverGlobalReadLimit;
				}
				if (wsl < 0) {
					wsl = Configuration.configuration.serverChannelWriteLimit;
				}
				if (rsl < 0) {
					rsl = Configuration.configuration.serverChannelReadLimit;
				}
				if (Configuration.configuration.r66Mib != null) {
					Configuration.configuration.r66Mib.notifyWarning(
							"Change Bandwidth Limit Order received: Global " +
									wgl + ":" + rgl + " (W:R) Local " + wsl + ":" + rsl + " (W:R)",
							session.getAuth().getUser());
				}
				Configuration.configuration.changeNetworkLimit(wgl, rgl, wsl, rsl,
						Configuration.configuration.delayLimit);
				R66Result result = new R66Result(session, true, ErrorCode.CompleteOk, null);
				// Now answer
				ValidPacket valid = new ValidPacket("Bandwidth changed", result.code.getCode(),
						LocalPacketFactory.REQUESTUSERPACKET);
				localChannelReference.validateRequest(result);
				try {
					ChannelUtils.writeAbstractLocalPacket(localChannelReference,
							valid, true);
				} catch (OpenR66ProtocolPacketException e) {
				}
				Channels.close(channel);
				break;
			}
			case LocalPacketFactory.TESTPACKET: {
				session.newState(VALIDOTHER);
				logger.info("Valid TEST MESSAGE: " + packet.toString());
				R66Result resulttest = new R66Result(session, true,
						ErrorCode.CompleteOk, null);
				resulttest.other = packet;
				localChannelReference.validateRequest(resulttest);
				Channels.close(channel);
				break;
			}
			default:
				logger.info("Validation is ignored: " + packet.getTypeValid());
		}
	}

	/**
	 * Receive an End of Request
	 * 
	 * @param channel
	 * @param packet
	 * @throws OpenR66RunnerErrorException
	 * @throws OpenR66ProtocolSystemException
	 * @throws OpenR66ProtocolNotAuthenticatedException
	 */
	private void endRequest(Channel channel, EndRequestPacket packet) {
		// Validate the last post action on a transfer from receiver remote host
		logger.info("Valid Request {}\nPacket {}",
				localChannelReference,
				packet);
		DbTaskRunner runner = session.getRunner();
		logger.debug("Runner endRequest: " + (session.getRunner() != null));
		if (runner != null) {
			runner.setAllDone();
			try {
				runner.saveStatus();
			} catch (OpenR66RunnerErrorException e) {
				// ignore
			}
		}
		String optional = null;
		if (session.getExtendedProtocol()) {
			optional = packet.getOptional();
		}
		if (!localChannelReference.getFutureRequest().isDone()) {
			// end of request
			R66Future transfer = localChannelReference.getFutureEndTransfer();
			try {
				transfer.await();
			} catch (InterruptedException e) {
			}
			if (transfer.isSuccess()) {
				if (session.getExtendedProtocol() && session.getBusinessObject() != null) {
					if (session.getBusinessObject().getInfo() == null) {
						session.getBusinessObject().setInfo(optional);
					} else {
						String temp = session.getBusinessObject().getInfo();
						session.getBusinessObject().setInfo(optional);
						optional = temp;
					}
				} else if (session.getExtendedProtocol() &&
						transfer.getResult().other == null && optional != null) {
					transfer.getResult().other = optional;
				}
				localChannelReference.validateRequest(transfer.getResult());
			}
		}
		session.setStatus(1);
		if (packet.isToValidate()) {
			session.newState(ENDREQUESTS);
			packet.validate();
			if (session.getExtendedProtocol()) {
				packet.setOptional(optional);
			}
			session.newState(ENDREQUESTR);
			try {
				ChannelUtils.writeAbstractLocalPacket(localChannelReference,
						packet, true);
			} catch (OpenR66ProtocolPacketException e) {
			}
		} else {
			session.newState(ENDREQUESTR);
		}
		if (runner != null && runner.isSelfRequested()) {
			ChannelCloseTimer.closeFutureChannel(channel);
		}
	}

	/**
	 * Receive a Shutdown request
	 * 
	 * @param channel
	 * @param packet
	 * @throws OpenR66ProtocolShutdownException
	 * @throws OpenR66ProtocolNotAuthenticatedException
	 * @throws OpenR66ProtocolBusinessException
	 */
	private void shutdown(Channel channel, ShutdownPacket packet)
			throws OpenR66ProtocolShutdownException,
			OpenR66ProtocolNotAuthenticatedException,
			OpenR66ProtocolBusinessException {
		if (!session.isAuthenticated()) {
			throw new OpenR66ProtocolNotAuthenticatedException(
					"Not authenticated while Shutdown received");
		}
		// SYSTEM authorization
		boolean isAdmin = session.getAuth().isValidRole(ROLE.SYSTEM);
		boolean isKeyValid = Configuration.configuration.isKeyValid(packet.getKey());
		if (isAdmin && isKeyValid) {
			if (Configuration.configuration.r66Mib != null) {
				Configuration.configuration.r66Mib.notifyStartStop(
						"Shutdown Order received effective in " +
								Configuration.configuration.TIMEOUTCON + " ms",
						session.getAuth().getUser());
			}
			if (Configuration.configuration.shutdownConfiguration.serviceFuture != null) {
				logger.warn("R66 started as a service, Windows Services might not shown it as stopped");
			}
			throw new OpenR66ProtocolShutdownException("Shutdown Type received");
		}
		logger.error("Invalid Shutdown command: from " + session.getAuth().getUser()
				+ " AdmValid: " + isAdmin + " KeyValid: " + isKeyValid);
		throw new OpenR66ProtocolBusinessException("Invalid Shutdown comand");
	}

	/**
	 * Business Request (channel should stay open)
	 * 
	 * Note: the thread called should manage all writeback informations, as well as status, channel
	 * closing if needed or not.
	 * 
	 * @param channel
	 * @param packet
	 * @throws OpenR66ProtocolNotAuthenticatedException
	 * @throws OpenR66ProtocolPacketException
	 */
	private void businessRequest(Channel channel, BusinessRequestPacket packet)
			throws OpenR66ProtocolNotAuthenticatedException,
			OpenR66ProtocolPacketException {
		if (!session.isAuthenticated()) {
			throw new OpenR66ProtocolNotAuthenticatedException(
					"Not authenticated while BusinessRequest received");
		}
		if (!Configuration.configuration.businessWhiteSet.contains(session.getAuth().getUser())) {
			throw new OpenR66ProtocolNotAuthenticatedException(
					"Not allow to execute a BusinessRequest");
		}
		session.setStatus(200);
		String argRule = packet.getSheader();
		int delay = packet.getDelay();
		boolean argTransfer = packet.isToValidate();
		if (argTransfer) {
			session.newState(BUSINESSD);
		}
		ExecJavaTask task = new ExecJavaTask(argRule + " " +
				AbstractBusinessRequest.BUSINESSREQUEST + " " + argTransfer,
				delay, null, session);
		task.run();
		session.setStatus(201);
		if (task.isSuccess()) {
			session.setStatus(202);
			logger.info("Task done: " + argRule);
		} else {
			R66Result result = task.getFutureCompletion().getResult();
			if (result == null) {
				result = new R66Result(session, false, ErrorCode.ExternalOp, session.getRunner());
			}
			logger.info("Task in Error:" + argRule + "\n" + result);
			if (!result.isAnswered) {
				packet.invalidate();
				session.newState(ERROR);
				ErrorPacket error = new ErrorPacket(
						"BusinessRequest in error: for " + packet.toString() + " since " +
								result.getMessage(),
						result.code.getCode(), ErrorPacket.FORWARDCLOSECODE);
				ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
				session.setStatus(203);
			}
			session.setStatus(204);
		}
	}

	/**
	 * Block/Unblock Request 
	 * 
	 * 
	 * @param channel
	 * @param packet
	 * @throws OpenR66ProtocolPacketException
	 * @throws OpenR66ProtocolBusinessException 
	 */
	private void blockRequest(Channel channel, BlockRequestPacket packet)
			throws OpenR66ProtocolPacketException, OpenR66ProtocolBusinessException {
		if (!session.isAuthenticated()) {
			throw new OpenR66ProtocolNotAuthenticatedException(
					"Not authenticated while BlockRequest received");
		}
		// SYSTEM authorization
		boolean isAdmin = session.getAuth().isValidRole(ROLE.SYSTEM);
		boolean isKeyValid = Configuration.configuration.isKeyValid(packet.getKey());
		if (isAdmin && isKeyValid) {
			boolean block = packet.getBlock();
			if (Configuration.configuration.r66Mib != null) {
				Configuration.configuration.r66Mib.notifyWarning(
						(block ? "Block" : "Unblock") + " Order received",
						session.getAuth().getUser());
			}
			logger.debug((block ? "Block" : "Unblock") + " Order received");
			Configuration.configuration.isShutdown = block;
			// inform back the requester
			// request of current values
			R66Result result = new R66Result(session, true, ErrorCode.CompleteOk, null);
			ValidPacket valid = new ValidPacket((block ? "Block" : "Unblock")+" new request", result.code.getCode(),
					LocalPacketFactory.REQUESTUSERPACKET);
			localChannelReference.validateRequest(result);
			try {
				ChannelUtils.writeAbstractLocalPacket(localChannelReference,
						valid, true);
			} catch (OpenR66ProtocolPacketException e) {
			}
			Channels.close(channel);
			return;
		}
		logger.error("Invalid Block command: from " + session.getAuth().getUser()
				+ " AdmValid: " + isAdmin + " KeyValid: " + isKeyValid);
		throw new OpenR66ProtocolBusinessException("Invalid Block comand");
	}

	/**
	 * Try to finalize the request if possible
	 * 
	 * @param errorValue
	 *            in case of Error
	 * @throws OpenR66ProtocolSystemException
	 * @throws OpenR66RunnerErrorException
	 */
	private void tryFinalizeRequest(R66Result errorValue)
			throws OpenR66RunnerErrorException, OpenR66ProtocolSystemException {
		session.tryFinalizeRequest(errorValue);
	}
}
