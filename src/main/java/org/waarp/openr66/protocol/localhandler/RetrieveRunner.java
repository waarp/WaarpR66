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

import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.waarp.common.file.DataBlock;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66FiniteDualStates;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;
import org.waarp.openr66.database.data.DbTaskRunner.TASKSTEP;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66Exception;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;
import org.waarp.openr66.protocol.localhandler.packet.EndRequestPacket;
import org.waarp.openr66.protocol.localhandler.packet.ErrorPacket;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelUtils;

/**
 * Retrieve transfer runner
 * 
 * @author Frederic Bregier
 * 
 */
public class RetrieveRunner extends Thread {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(RetrieveRunner.class);

	private final R66Session session;

	private final LocalChannelReference localChannelReference;

	private final Channel channel;

	private boolean done = false;

	protected AtomicBoolean running = new AtomicBoolean(true);

	protected RetrieveRunner() {
		// empty constructor
		this.session = null;
		this.localChannelReference = null;
		this.channel = null;
	}

	/**
	 * 
	 * @param session
	 * @param channel
	 *            local channel
	 */
	public RetrieveRunner(R66Session session, Channel channel) {
		this.session = session;
		localChannelReference = this.session.getLocalChannelReference();
		this.channel = channel;
	}

	/**
	 * Try to stop the runner
	 */
	public void stopRunner() {
		running.set(false);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		boolean requestValidDone = false;
		try {
			Thread.currentThread().setName("RetrieveRunner: " + channel.getId());
			try {
				if (session.getRunner().getGloballaststep() == TASKSTEP.POSTTASK.ordinal()) {
					logger.debug("Restart from POSTTASK: EndTransfer");
					// restart from PostTask global step so just end now
					try {
						ChannelUtils.writeEndTransfer(localChannelReference);
					} catch (OpenR66ProtocolPacketException e) {
						transferInError(e);
						logger.error("End Retrieve in Error");
						return;
					}
				} else {
					logger.debug("Start retrieve operation (send)");
					session.getFile().retrieveBlocking(running);
				}
			} catch (OpenR66RunnerErrorException e) {
				transferInError(e);
				logger.info("End Retrieve in Error");
				return;
			} catch (OpenR66ProtocolSystemException e) {
				transferInError(e);
				logger.info("End Retrieve in Error");
				return;
			}
			if (running.get()) {
				try {
					localChannelReference.getFutureEndTransfer().await();
				} catch (InterruptedException e1) {
				}
			}
			logger.debug("Await future End Transfer done: " +
					localChannelReference.getFutureEndTransfer().isSuccess());
			if (localChannelReference.getFutureEndTransfer().isDone() &&
					localChannelReference.getFutureEndTransfer().isSuccess()) {
				// send a validation
				requestValidDone = true;
				localChannelReference.sessionNewState(R66FiniteDualStates.ENDREQUESTS);
				EndRequestPacket validPacket = new EndRequestPacket(ErrorCode.CompleteOk.ordinal());
				if (session.getExtendedProtocol() &&
						session.getBusinessObject() != null &&
						session.getBusinessObject().getInfo() != null) {
					validPacket.setOptional(session.getBusinessObject().getInfo());
				}
				try {
					ChannelUtils.writeAbstractLocalPacket(localChannelReference, validPacket, true);
				} catch (OpenR66ProtocolPacketException e) {
				}
				if (!localChannelReference.getFutureRequest().awaitUninterruptibly(
						Configuration.configuration.TIMEOUTCON)) {
					// valid it however
					session.getRunner().setAllDone();
					try {
						session.getRunner().saveStatus();
					} catch (OpenR66RunnerErrorException e) {
						// ignore
					}
					localChannelReference.validateRequest(localChannelReference
							.getFutureEndTransfer().getResult());
				}
				if (session.getRunner() != null && session.getRunner().isSelfRequested()) {
					ChannelUtils.close(localChannelReference.getLocalChannel());
				}
				done = true;
			} else {
				if (localChannelReference.getFutureEndTransfer().isDone()) {
					// Done and Not Success => error
					if (!localChannelReference.getFutureEndTransfer().getResult().isAnswered) {
						localChannelReference.sessionNewState(R66FiniteDualStates.ERROR);
						ErrorPacket error = new ErrorPacket(
								localChannelReference.getErrorMessage(),
								localChannelReference.getFutureEndTransfer().getResult().code
										.getCode(),
								ErrorPacket.FORWARDCLOSECODE);
						try {
							ChannelUtils.writeAbstractLocalPacket(localChannelReference, error,
									true);
						} catch (OpenR66ProtocolPacketException e) {
						}
					}
				}
				if (!localChannelReference.getFutureRequest().isDone()) {
					R66Result result = localChannelReference.getFutureEndTransfer().getResult();
					if (result == null) {
						result =
								new R66Result(session, false, ErrorCode.TransferError,
										session.getRunner());
					}
					localChannelReference.invalidateRequest(result);
				}
				done = true;
				logger.info("End Retrieve in Error");
			}
		} finally {
			if (!done) {
				if (localChannelReference.getFutureEndTransfer().isDone() &&
						localChannelReference.getFutureEndTransfer().isSuccess()) {
					if (!requestValidDone) {
						localChannelReference.sessionNewState(R66FiniteDualStates.ENDREQUESTS);
						EndRequestPacket validPacket = new EndRequestPacket(
								ErrorCode.CompleteOk.ordinal());
						if (session.getExtendedProtocol() &&
								session.getBusinessObject() != null &&
								session.getBusinessObject().getInfo() != null) {
							validPacket.setOptional(session.getBusinessObject().getInfo());
						}
						try {
							ChannelUtils.writeAbstractLocalPacket(localChannelReference,
									validPacket, true);
						} catch (OpenR66ProtocolPacketException e) {
						}
					}
					session.getRunner().setAllDone();
					try {
						session.getRunner().saveStatus();
					} catch (OpenR66RunnerErrorException e) {
						// ignore
					}
					localChannelReference.validateRequest(localChannelReference
							.getFutureEndTransfer().getResult());
					if (session.getRunner() != null && session.getRunner().isSelfRequested()) {
						ChannelUtils.close(localChannelReference.getLocalChannel());
					}
				} else {
					if (localChannelReference.getFutureEndTransfer().isDone()) {
						if (!localChannelReference.getFutureEndTransfer().getResult().isAnswered) {
							localChannelReference.sessionNewState(R66FiniteDualStates.ERROR);
							ErrorPacket error = new ErrorPacket(
									localChannelReference.getErrorMessage(),
									localChannelReference.getFutureEndTransfer().getResult().code
											.getCode(),
									ErrorPacket.FORWARDCLOSECODE);
							try {
								ChannelUtils.writeAbstractLocalPacket(localChannelReference, error,
										true);
							} catch (OpenR66ProtocolPacketException e) {
							}
						}
					} else {
						R66Result result = localChannelReference.getFutureEndTransfer().getResult();
						if (result == null) {
							result =
									new R66Result(session, false, ErrorCode.TransferError,
											session.getRunner());
						}
						localChannelReference.invalidateRequest(result);
					}
				}
			}
			NetworkTransaction.normalEndRetrieve(localChannelReference);
		}
	}

	private void transferInError(OpenR66Exception e) {
		R66Result result = new R66Result(e, session, true,
				ErrorCode.TransferError, session.getRunner());
		logger.error("Transfer in error", e);
		session.newState(R66FiniteDualStates.ERROR);
		ErrorPacket error = new ErrorPacket("Transfer in error",
				ErrorCode.TransferError.getCode(), ErrorPacket.FORWARDCLOSECODE);
		try {
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
		} catch (OpenR66ProtocolPacketException e1) {
		}
		localChannelReference.invalidateRequest(result);
		ChannelUtils.close(channel);
		done = true;
	}

	/**
	 * Write the next block when the channel is ready to prevent OOM
	 * 
	 * @param block
	 * @param localChannelReference
	 * @return the ChannelFuture on the write operation
	 * @throws OpenR66ProtocolPacketException
	 * @throws OpenR66RunnerErrorException
	 * @throws OpenR66ProtocolSystemException
	 */
	public static ChannelFuture writeWhenPossible(
			DataBlock block, LocalChannelReference localChannelReference)
			throws OpenR66ProtocolPacketException, OpenR66RunnerErrorException,
			OpenR66ProtocolSystemException {
		return ChannelUtils.writeBackDataBlock(localChannelReference, block);
		// XXX Keep this in case the bug comes back
		/*
		 * // Test if channel is writable in order to prevent OOM if (!
		 * localChannelReference.getNetworkChannel().isWritable()) { return
		 * ChannelUtils.writeBackDataBlock(localChannelReference, block); } else if
		 * (Configuration.configuration.anyBandwidthLimitation) { // Patch to limit the impact when
		 * no real reason to wait for writing // double computation of traffic but ok long wait =
		 * ChannelUtils.willBeWaitingWriting(localChannelReference, block.getByteCount()); if (wait
		 * == 0) { ChannelUtils.writeBackDataBlock(localChannelReference, block); return
		 * Channels.succeededFuture(localChannelReference.getNetworkChannel()); } return
		 * ChannelUtils.writeBackDataBlock(localChannelReference, block); } else {
		 * ChannelUtils.writeBackDataBlock(localChannelReference, block); return
		 * Channels.succeededFuture(localChannelReference.getNetworkChannel()); }
		 */
	}

	/**
	 * Utility method for send through mode
	 * 
	 * @param data
	 *            the data byte, if null it is the last block
	 * @return the DataBlock associated to the data
	 */
	public static DataBlock transformToDataBlock(byte[] data) {
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
