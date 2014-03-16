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

import static org.waarp.openr66.context.R66FiniteDualStates.DATAR;
import static org.waarp.openr66.context.R66FiniteDualStates.ERROR;
import static org.waarp.openr66.context.R66FiniteDualStates.INFORMATION;
import static org.waarp.openr66.context.R66FiniteDualStates.SHUTDOWN;
import static org.waarp.openr66.context.R66FiniteDualStates.TEST;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66FiniteDualStates;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;
import org.waarp.openr66.context.task.exception.OpenR66RunnerException;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.Messages;
import org.waarp.openr66.protocol.configuration.PartnerConfiguration;
import org.waarp.openr66.protocol.exception.OpenR66Exception;
import org.waarp.openr66.protocol.exception.OpenR66ExceptionTrappedFactory;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessCancelException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessNoWriteBackException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessQueryAlreadyFinishedException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessQueryStillRunningException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessRemoteFileNotFoundException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessStopException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNetworkException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNotAuthenticatedException;
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
import org.waarp.openr66.protocol.localhandler.packet.JsonCommandPacket;
import org.waarp.openr66.protocol.localhandler.packet.LocalPacketFactory;
import org.waarp.openr66.protocol.localhandler.packet.RequestPacket;
import org.waarp.openr66.protocol.localhandler.packet.ShutdownPacket;
import org.waarp.openr66.protocol.localhandler.packet.StartupPacket;
import org.waarp.openr66.protocol.localhandler.packet.TestPacket;
import org.waarp.openr66.protocol.localhandler.packet.ValidPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.JsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.RequestJsonPacket;
import org.waarp.openr66.protocol.utils.ChannelCloseTimer;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66ShutdownHook;

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
	 * Server Actions handler
	 */
	private TransferActions serverHandler = new TransferActions();
	
	/*
	 * (non-Javadoc)
	 * @see org.jboss.netty.channel.SimpleChannelHandler#channelClosed(org.jboss.
	 * netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelStateEvent)
	 */
	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
		serverHandler.channelClosed(e);
	}

	/*
	 * (non-Javadoc)
	 * @see org.jboss.netty.channel.SimpleChannelHandler#channelConnected(org.jboss
	 * .netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelStateEvent)
	 */
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
		serverHandler.newSession();
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
			serverHandler.startup(e.getChannel(), (StartupPacket) packet);
		} else {
			if (serverHandler.getLocalChannelReference() == null) {
				logger.error("No LocalChannelReference at " +
						packet.getClass().getName());
				serverHandler.getSession().newState(ERROR);
				final ErrorPacket errorPacket = new ErrorPacket(
						"No LocalChannelReference at " +
								packet.getClass().getName(),
						ErrorCode.ConnectionImpossible.getCode(),
						ErrorPacket.FORWARDCLOSECODE);
				try {
					Channels.write(e.getChannel(), errorPacket).await();
				} catch (InterruptedException e1) {
				}
				ChannelUtils.close(e.getChannel());
				if (Configuration.configuration.r66Mib != null) {
					Configuration.configuration.r66Mib.notifyWarning(
							"No LocalChannelReference", packet.getClass().getSimpleName());
				}
				return;
			}
			switch (packet.getType()) {
				case LocalPacketFactory.AUTHENTPACKET: {
					serverHandler.authent(e.getChannel(), (AuthentPacket) packet);
					break;
				}
				// Already done case LocalPacketFactory.STARTUPPACKET:
				case LocalPacketFactory.DATAPACKET: {
					if (((DataPacket) packet).getPacketRank() % 100 == 1 || serverHandler.getSession().getState() != R66FiniteDualStates.DATAR) {
						serverHandler.getSession().newState(DATAR);
						logger.debug("DATA RANK: " + ((DataPacket) packet).getPacketRank() + " : " +
								serverHandler.getSession().getRunner().getRank());
					}
					serverHandler.data(e.getChannel(), (DataPacket) packet);
					break;
				}
				case LocalPacketFactory.VALIDPACKET: {
					// SHUTDOWNPACKET does not need authentication
					if (((ValidPacket) packet).getTypeValid() != LocalPacketFactory.SHUTDOWNPACKET &&
							(!serverHandler.getSession().isAuthenticated())) {
						logger.warn("Valid packet received while not authenticated: {} {}", packet, serverHandler.getSession());
						serverHandler.getSession().newState(ERROR);
						throw new OpenR66ProtocolNotAuthenticatedException(
								"Not authenticated while Valid received");
					}
					if (((ValidPacket) packet).getTypeValid() == LocalPacketFactory.REQUESTPACKET) {
						String [] fields = ((ValidPacket) packet).getSmiddle().split(PartnerConfiguration.BAR_SEPARATOR_FIELD);
						String newfilename = fields[0];
						// potential file size changed
						long newSize = -1;
						if (fields.length > 1) {
							try {
								newSize = Long.parseLong(fields[fields.length-1]);
							} catch (NumberFormatException e2) {
								newfilename += PartnerConfiguration.BAR_SEPARATOR_FIELD + fields[fields.length-1];
								newSize = -1;
							}
						}
						serverHandler.requestChangeNameSize(e.getChannel(), newfilename, newSize);
					} else {
						serverHandler.valid(e.getChannel(), (ValidPacket) packet);
					}
					break;
				}
				case LocalPacketFactory.ERRORPACKET: {
					serverHandler.getSession().newState(ERROR);
					serverHandler.errorMesg(e.getChannel(), (ErrorPacket) packet);
					break;
				}
				case LocalPacketFactory.CONNECTERRORPACKET: {
					serverHandler.connectionError(e.getChannel(),
							(ConnectionErrorPacket) packet);
					break;
				}
				case LocalPacketFactory.REQUESTPACKET: {
					serverHandler.request(e.getChannel(), (RequestPacket) packet);
					break;
				}
				case LocalPacketFactory.SHUTDOWNPACKET: {
					serverHandler.getSession().newState(SHUTDOWN);
					serverHandler.shutdown(e.getChannel(), (ShutdownPacket) packet);
					break;
				}
				case LocalPacketFactory.STOPPACKET:
				case LocalPacketFactory.CANCELPACKET:
				case LocalPacketFactory.CONFIMPORTPACKET:
				case LocalPacketFactory.CONFEXPORTPACKET:
				case LocalPacketFactory.BANDWIDTHPACKET: {
					logger.error("Unimplemented Mesg: " +
							packet.getClass().getName());
					serverHandler.getSession().newState(ERROR);
					serverHandler.getLocalChannelReference().invalidateRequest(new R66Result(
							new OpenR66ProtocolSystemException(
									"Not implemented"), serverHandler.getSession(), true,
							ErrorCode.Unimplemented, null));
					final ErrorPacket errorPacket = new ErrorPacket(
							"Unimplemented Mesg: " +
									packet.getClass().getName(),
							ErrorCode.Unimplemented.getCode(),
							ErrorPacket.FORWARDCLOSECODE);
					ChannelUtils.writeAbstractLocalPacket(serverHandler.getLocalChannelReference(), errorPacket, true);
					ChannelUtils.close(e.getChannel());
					break;
				}
				case LocalPacketFactory.TESTPACKET: {
					serverHandler.getSession().newState(TEST);
					serverHandler.test(e.getChannel(), (TestPacket) packet);
					break;
				}
				case LocalPacketFactory.ENDTRANSFERPACKET: {
					serverHandler.endTransfer(e.getChannel(), (EndTransferPacket) packet);
					break;
				}
				case LocalPacketFactory.INFORMATIONPACKET: {
					serverHandler.getSession().newState(INFORMATION);
					serverHandler.information(e.getChannel(), (InformationPacket) packet);
					break;
				}
				case LocalPacketFactory.ENDREQUESTPACKET: {
					serverHandler.endRequest(e.getChannel(), (EndRequestPacket) packet);
					break;
				}
				case LocalPacketFactory.BUSINESSREQUESTPACKET: {
					serverHandler.businessRequest(e.getChannel(), (BusinessRequestPacket) packet);
					break;
				}
				case LocalPacketFactory.BLOCKREQUESTPACKET: {
					serverHandler.blockRequest(e.getChannel(), (BlockRequestPacket) packet);
					break;
				}
				case LocalPacketFactory.JSONREQUESTPACKET: {
					if (!serverHandler.getSession().isAuthenticated()) {
						logger.warn("JsonCommand packet received while not authenticated: {} {}", packet, serverHandler.getSession());
						serverHandler.getSession().newState(ERROR);
						throw new OpenR66ProtocolNotAuthenticatedException(
								"Not authenticated while Valid received");
					}
					JsonPacket json = ((JsonCommandPacket) packet).getJsonRequest();
					if (json == null) {
						ErrorCode code = ErrorCode.CommandNotFound;
						R66Result resulttest = new R66Result(serverHandler.getSession(), true,
								code, serverHandler.getSession().getRunner());
						json = new JsonPacket();
						json.setComment("Invalid command");
						json.setRequestUserPacket(((JsonCommandPacket) packet).getTypeValid());
						JsonCommandPacket valid = new JsonCommandPacket(json, resulttest.code.getCode(),
								LocalPacketFactory.REQUESTUSERPACKET);
						resulttest.other = packet;
						serverHandler.getLocalChannelReference().validateRequest(resulttest);
						try {
							ChannelUtils.writeAbstractLocalPacket(serverHandler.getLocalChannelReference(),
									valid, true);
						} catch (OpenR66ProtocolPacketException e2) {
						}
						serverHandler.getSession().setStatus(99);
						Channels.close(e.getChannel());
						return;
					}
					json.setRequestUserPacket(((JsonCommandPacket) packet).getTypeValid());
					if (((JsonCommandPacket) packet).getTypeValid() == LocalPacketFactory.REQUESTPACKET) {
						RequestJsonPacket node = (RequestJsonPacket) json;
						String newfilename = node.getFilename();
						if (newfilename == null) {
							// error so ignore
							return;
						}
						long newSize = node.getFilesize();
						logger.debug("NewSize "+ newSize + " NewName "+newfilename);
						// potential file size changed
						serverHandler.requestChangeNameSize(e.getChannel(), newfilename, newSize);
					} else {
						serverHandler.jsonCommand(e.getChannel(), (JsonCommandPacket) packet);
					}
					break;
				}
				default: {
					logger
							.error("Unknown Mesg: " +
									packet.getClass().getName());
					serverHandler.getSession().newState(ERROR);
					serverHandler.getLocalChannelReference().invalidateRequest(new R66Result(
							new OpenR66ProtocolSystemException(
									"Unknown Message"), serverHandler.getSession(), true,
							ErrorCode.Unimplemented, null));
					final ErrorPacket errorPacket = new ErrorPacket(
							"Unkown Mesg: " + packet.getClass().getName(),
							ErrorCode.Unimplemented.getCode(), ErrorPacket.FORWARDCLOSECODE);
					ChannelUtils.writeAbstractLocalPacket(serverHandler.getLocalChannelReference(), errorPacket, true);
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
				(serverHandler.getLocalChannelReference() != null && serverHandler.getLocalChannelReference().getFutureRequest().isDone()), e.getCause());
		if (serverHandler.getLocalChannelReference() != null && serverHandler.getLocalChannelReference().getFutureRequest().isDone()) {
			Channels.close(e.getChannel());
			return;
		}
		OpenR66Exception exception = OpenR66ExceptionTrappedFactory
				.getExceptionFromTrappedException(e.getChannel(), e);
		ErrorCode code = null;
		if (exception != null) {
			serverHandler.getSession().newState(ERROR);
			boolean isAnswered = false;
			if (exception instanceof OpenR66ProtocolShutdownException) {
				R66ShutdownHook.shutdownWillStart();
				logger.warn(Messages.getString("LocalServerHandler.0") + //$NON-NLS-1$
						serverHandler.getSession().getAuth().getUser());
				if (serverHandler.getLocalChannelReference() != null) {
					R66Result finalValue = new R66Result(exception, serverHandler.getSession(), true,
							ErrorCode.Shutdown, serverHandler.getSession().getRunner());
					try {
						serverHandler.tryFinalizeRequest(finalValue);
					} catch (OpenR66RunnerErrorException e2) {
					} catch (OpenR66ProtocolSystemException e2) {
					}
					if (!serverHandler.getLocalChannelReference().getFutureRequest().isDone()) {
						try {
							serverHandler.getSession().setFinalizeTransfer(false, finalValue);
						} catch (OpenR66RunnerErrorException e1) {
							serverHandler.getLocalChannelReference().invalidateRequest(finalValue);
						} catch (OpenR66ProtocolSystemException e1) {
							serverHandler.getLocalChannelReference().invalidateRequest(finalValue);
						}
					}
				}
				// dont'close, thread will do
				ChannelUtils.startShutdown();
				// set global shutdown info and before close, send a valid
				// shutdown to all
				serverHandler.getSession().setStatus(54);
				return;
			} else {
				if (serverHandler.getLocalChannelReference() != null
						&& serverHandler.getLocalChannelReference().getFutureRequest() != null) {
					if (serverHandler.getLocalChannelReference().getFutureRequest().isDone()) {
						R66Result result = serverHandler.getLocalChannelReference().getFutureRequest()
								.getResult();
						if (result != null) {
							isAnswered = result.isAnswered;
						}
					}
				}
				if (exception instanceof OpenR66ProtocolNoConnectionException) {
					code = ErrorCode.ConnectionImpossible;
					DbTaskRunner runner = serverHandler.getSession().getRunner();
					if (runner != null) {
						runner.stopOrCancelRunner(code);
					}
				} else if (exception instanceof OpenR66ProtocolBusinessCancelException) {
					code = ErrorCode.CanceledTransfer;
					DbTaskRunner runner = serverHandler.getSession().getRunner();
					if (runner != null) {
						runner.stopOrCancelRunner(code);
					}
				} else if (exception instanceof OpenR66ProtocolBusinessStopException) {
					code = ErrorCode.StoppedTransfer;
					DbTaskRunner runner = serverHandler.getSession().getRunner();
					if (runner != null) {
						runner.stopOrCancelRunner(code);
					}
				} else if (exception instanceof OpenR66ProtocolBusinessQueryAlreadyFinishedException) {
					code = ErrorCode.QueryAlreadyFinished;
					try {
						serverHandler.tryFinalizeRequest(new R66Result(serverHandler.getSession(), true, code, serverHandler.getSession().getRunner()));
						ChannelCloseTimer.closeFutureChannel(e.getChannel());
						return;
					} catch (OpenR66RunnerErrorException e1) {
					} catch (OpenR66ProtocolSystemException e1) {
					}
				} else if (exception instanceof OpenR66ProtocolBusinessQueryStillRunningException) {
					code = ErrorCode.QueryStillRunning;
					// nothing is to be done
					logger.error("Will close channel since ", exception);
					Channels.close(e.getChannel());
					serverHandler.getSession().setStatus(56);
					return;
				} else if (exception instanceof OpenR66ProtocolBusinessRemoteFileNotFoundException) {
					code = ErrorCode.FileNotFound;
				} else if (exception instanceof OpenR66RunnerException) {
					code = ErrorCode.ExternalOp;
				} else if (exception instanceof OpenR66RunnerErrorException) {
					code = ErrorCode.ExternalOp;
				} else if (exception instanceof OpenR66ProtocolNotAuthenticatedException) {
					code = ErrorCode.BadAuthent;
				} else if (exception instanceof OpenR66ProtocolNetworkException) {
					code = ErrorCode.Disconnection;
					DbTaskRunner runner = serverHandler.getSession().getRunner();
					if (runner != null) {
						R66Result finalValue = new R66Result(
								new OpenR66ProtocolSystemException(
										Messages.getString("LocalServerHandler.2")), //$NON-NLS-1$
								serverHandler.getSession(), true, code, serverHandler.getSession().getRunner());
						try {
							serverHandler.tryFinalizeRequest(finalValue);
						} catch (OpenR66Exception e2) {
						}
					}
				} else if (exception instanceof OpenR66ProtocolRemoteShutdownException) {
					code = ErrorCode.RemoteShutdown;
					DbTaskRunner runner = serverHandler.getSession().getRunner();
					if (runner != null) {
						runner.stopOrCancelRunner(code);
					}
				} else {
					DbTaskRunner runner = serverHandler.getSession().getRunner();
					if (runner != null) {
						switch (runner.getErrorInfo()) {
							case InitOk:
							case PostProcessingOk:
							case PreProcessingOk:
							case Running:
							case TransferOk:
								code = ErrorCode.Internal;
								break;
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
						if (serverHandler.getLocalChannelReference() != null) {
							ChannelUtils.writeAbstractLocalPacket(serverHandler.getLocalChannelReference(),
								errorPacket, true);
						}
					} catch (OpenR66ProtocolPacketException e1) {
						// should not be
					}
				}
				R66Result finalValue =
						new R66Result(
								exception, serverHandler.getSession(), true, code, serverHandler.getSession().getRunner());
				try {
					serverHandler.getSession().setFinalizeTransfer(false, finalValue);
					if (serverHandler.getLocalChannelReference() != null) {
						serverHandler.getLocalChannelReference().invalidateRequest(finalValue);
					}
				} catch (OpenR66RunnerErrorException e1) {
					if (serverHandler.getLocalChannelReference() != null)
						serverHandler.getLocalChannelReference().invalidateRequest(finalValue);
				} catch (OpenR66ProtocolSystemException e1) {
					if (serverHandler.getLocalChannelReference() != null)
						serverHandler.getLocalChannelReference().invalidateRequest(finalValue);
				}
			}
			if (exception instanceof OpenR66ProtocolBusinessNoWriteBackException) {
				logger.error("Will close channel {}", exception.getMessage());
				Channels.close(e.getChannel());
				serverHandler.getSession().setStatus(56);
				return;
			} else if (exception instanceof OpenR66ProtocolNoConnectionException) {
				logger.error("Will close channel {}", exception.getMessage());
				Channels.close(e.getChannel());
				serverHandler.getSession().setStatus(57);
				return;
			}
			serverHandler.getSession().setStatus(58);
			ChannelCloseTimer.closeFutureChannel(e.getChannel());
		} else {
			// Nothing to do
			serverHandler.getSession().setStatus(59);
			return;
		}
	}
}
