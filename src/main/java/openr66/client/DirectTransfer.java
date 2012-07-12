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
package openr66.client;

import goldengate.common.database.exception.GoldenGateDatabaseException;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;
import openr66.commander.ClientRunner;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.DbConstant;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolNotYetConnectionException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.ChannelUtils;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.logging.InternalLoggerFactory;

/**
 * Direct Transfer from a client with or without database connection
 * 
 * @author Frederic Bregier
 * 
 */
public class DirectTransfer extends AbstractTransfer {
	protected final NetworkTransaction networkTransaction;

	public DirectTransfer(R66Future future, String remoteHost,
			String filename, String rulename, String fileinfo, boolean isMD5, int blocksize,
			long id,
			NetworkTransaction networkTransaction) {
		// no starttime since it is direct (blocking request, no delay)
		super(DirectTransfer.class,
				future, filename, rulename, fileinfo, isMD5, remoteHost, blocksize, id, null);
		this.networkTransaction = networkTransaction;
	}

	/**
	 * Prior to call this method, the pipeline and NetworkTransaction must have been initialized. It
	 * is the responsibility of the caller to finish all network resources.
	 */
	public void run() {
		if (logger == null) {
			logger = GgInternalLoggerFactory.getLogger(DirectTransfer.class);
		}
		DbTaskRunner taskRunner = this.initRequest();
		ClientRunner runner = new ClientRunner(networkTransaction, taskRunner, future);
		OpenR66ProtocolNotYetConnectionException exc = null;
		for (int i = 0; i < Configuration.RETRYNB; i++) {
			try {
				runner.runTransfer();
				exc = null;
				break;
			} catch (OpenR66RunnerErrorException e) {
				logger.debug("Cannot Transfer", e);
				future.setResult(new R66Result(e, null, true,
						ErrorCode.Internal, taskRunner));
				future.setFailure(e);
				return;
			} catch (OpenR66ProtocolNoConnectionException e) {
				logger.debug("Cannot Connect", e);
				future.setResult(new R66Result(e, null, true,
						ErrorCode.ConnectionImpossible, taskRunner));
				// since no connection : just forget it
				if (nolog) {
					try {
						taskRunner.delete();
					} catch (GoldenGateDatabaseException e1) {
					}
				}
				future.setFailure(e);
				return;
			} catch (OpenR66ProtocolPacketException e) {
				logger.debug("Bad Protocol", e);
				future.setResult(new R66Result(e, null, true,
						ErrorCode.TransferError, taskRunner));
				future.setFailure(e);
				return;
			} catch (OpenR66ProtocolNotYetConnectionException e) {
				logger.debug("Not Yet Connected", e);
				exc = e;
				continue;
			}
		}
		if (exc != null) {
			taskRunner.setLocalChannelReference(new LocalChannelReference());
			logger.debug("Cannot Connect", exc);
			future.setResult(new R66Result(exc, null, true,
					ErrorCode.ConnectionImpossible, taskRunner));
			// since no connection : just forget it
			if (nolog) {
				try {
					taskRunner.delete();
				} catch (GoldenGateDatabaseException e1) {
				}
			}
			future.setFailure(exc);
			return;
		}
	}

	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(null));
		if (logger == null) {
			logger = GgInternalLoggerFactory.getLogger(DirectTransfer.class);
		}
		if (!getParams(args, false)) {
			logger.error("Wrong initialization");
			if (DbConstant.admin != null && DbConstant.admin.isConnected) {
				DbConstant.admin.close();
			}
			ChannelUtils.stopLogger();
			System.exit(2);
		}
		long time1 = System.currentTimeMillis();
		R66Future future = new R66Future(true);

		Configuration.configuration.pipelineInit();
		NetworkTransaction networkTransaction = new NetworkTransaction();
		try {
			DirectTransfer transaction = new DirectTransfer(future,
					rhost, localFilename, rule, fileInfo, ismd5, block, idt,
					networkTransaction);
			transaction.run();
			future.awaitUninterruptibly();
			long time2 = System.currentTimeMillis();
			logger.debug("finish transfer: " + future.isSuccess());
			long delay = time2 - time1;
			R66Result result = future.getResult();
			if (future.isSuccess()) {
				if (result.runner.getErrorInfo() == ErrorCode.Warning) {
					logger.warn("Transfer in status:\nWARNED\n    "
							+ result.runner.toShortString()
							+
							"\n    <REMOTE>"
							+ rhost
							+ "</REMOTE>"
							+
							"\n    <FILEFINAL>"
							+
							(result.file != null ? result.file.toString() + "</FILEFINAL>"
									: "no file")
							+ "\n    delay: " + delay);
				} else {
					logger.info("Transfer in status:\nSUCCESS\n    "
							+ result.runner.toShortString()
							+
							"\n    <REMOTE>"
							+ rhost
							+ "</REMOTE>"
							+
							"\n    <FILEFINAL>"
							+
							(result.file != null ? result.file.toString() + "</FILEFINAL>"
									: "no file")
							+ "\n    delay: " + delay);
				}
				if (nolog) {
					// In case of success, delete the runner
					try {
						result.runner.delete();
					} catch (GoldenGateDatabaseException e) {
						logger.warn("Cannot apply nolog to\n    " + result.runner.toShortString(),
								e);
					}
				}
			} else {
				if (result == null || result.runner == null) {
					logger.error("Transfer in\n    FAILURE with no Id", future.getCause());
					networkTransaction.closeAll();
					System.exit(ErrorCode.Unknown.ordinal());
				}
				if (result.runner.getErrorInfo() == ErrorCode.Warning) {
					logger.warn("Transfer is\n    WARNED\n    " + result.runner.toShortString() +
							"\n    <REMOTE>" + rhost + "</REMOTE>", future.getCause());
					networkTransaction.closeAll();
					System.exit(result.code.ordinal());
				} else {
					logger.error("Transfer in\n    FAILURE\n    " + result.runner.toShortString() +
							"\n    <REMOTE>" + rhost + "</REMOTE>", future.getCause());
					networkTransaction.closeAll();
					System.exit(result.code.ordinal());
				}
			}
		} catch (Exception e) {
			logger.debug("exc", e);
		} finally {
			logger.debug("finish transfer: " + future.isDone() + ":" + future.isSuccess());
			networkTransaction.closeAll();
			// In case something wrong append
			if (future.isDone() && future.isSuccess()) {
				System.exit(0);
			} else {
				System.exit(66);
			}
		}
	}

}
