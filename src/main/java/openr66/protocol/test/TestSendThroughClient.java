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
package openr66.protocol.test;

import goldengate.common.database.exception.GoldenGateDatabaseException;
import goldengate.common.exception.FileEndOfTransferException;
import goldengate.common.exception.FileTransferException;
import goldengate.common.file.DataBlock;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;
import openr66.client.SendThroughClient;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.filesystem.R66File;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.DbConstant;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * Test class for Send Through client
 * 
 * @author Frederic Bregier
 * 
 */
public class TestSendThroughClient extends SendThroughClient {

	/**
	 * @param future
	 * @param remoteHost
	 * @param filename
	 * @param rulename
	 * @param fileinfo
	 * @param isMD5
	 * @param blocksize
	 * @param networkTransaction
	 */
	public TestSendThroughClient(R66Future future, String remoteHost,
			String filename, String rulename, String fileinfo, boolean isMD5,
			int blocksize, NetworkTransaction networkTransaction) {
		super(future, remoteHost, filename, rulename, fileinfo, isMD5, blocksize,
				DbConstant.ILLEGALVALUE, networkTransaction);
	}

	public boolean sendFile() {
		R66File r66file = localChannelReference.getSession().getFile();
		boolean retrieveDone = false;
		try {
			DataBlock block = null;
			try {
				block = r66file.readDataBlock();
			} catch (FileEndOfTransferException e) {
				// Last block (in fact, no data to read)
				retrieveDone = true;
				return retrieveDone;
			}
			if (block == null) {
				// Last block (in fact, no data to read)
				retrieveDone = true;
				return retrieveDone;
			}
			// While not last block
			ChannelFuture future = null;
			while (block != null && !block.isEOF()) {
				future = this.writeWhenPossible(block);
				try {
					block = r66file.readDataBlock();
				} catch (FileEndOfTransferException e) {
					// Wait for last write
					future.awaitUninterruptibly();
					retrieveDone = true;
					return future.isSuccess();
				}
				future.awaitUninterruptibly();
				if (future.isCancelled()) {
					return false;
				}
			}
			// Last block
			if (block != null) {
				future = this.writeWhenPossible(block);
			}
			// Wait for last write
			if (future != null) {
				future.awaitUninterruptibly();
				return future.isSuccess();
			}
			retrieveDone = true;
			return retrieveDone;
		} catch (FileTransferException e) {
			// An error occurs!
			this.transferInError(new OpenR66ProtocolSystemException(e));
			return false;
		} catch (OpenR66ProtocolPacketException e) {
			// An error occurs!
			this.transferInError(e);
			return false;
		} catch (OpenR66RunnerErrorException e) {
			// An error occurs!
			this.transferInError(e);
			return false;
		} catch (OpenR66ProtocolSystemException e) {
			// An error occurs!
			this.transferInError(e);
			return false;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
				Level.WARN));
		if (logger == null) {
			logger = GgInternalLoggerFactory.getLogger(TestSendThroughClient.class);
		}
		if (!getParams(args, false)) {
			logger.error("Wrong initialization");
			if (DbConstant.admin != null && DbConstant.admin.isConnected) {
				DbConstant.admin.close();
			}
			System.exit(1);
		}
		Configuration.configuration.pipelineInit();
		NetworkTransaction networkTransaction = new NetworkTransaction();
		try {
			R66Future future = new R66Future(true);
			TestSendThroughClient transaction = new TestSendThroughClient(future,
					rhost, localFilename, rule, fileInfo, ismd5, block,
					networkTransaction);
			long time1 = System.currentTimeMillis();
			if (!transaction.initiateRequest()) {
				logger.error("Transfer in Error", future.getCause());
				return;
			}
			if (transaction.sendFile()) {
				transaction.finalizeRequest();
			} else {
				transaction.transferInError(null);
			}
			future.awaitUninterruptibly();

			long time2 = System.currentTimeMillis();
			long delay = time2 - time1;
			R66Result result = future.getResult();
			if (future.isSuccess()) {
				if (result.runner.getErrorInfo() == ErrorCode.Warning) {
					logger.warn("Warning with Id: " +
							result.runner.getSpecialId() + " on file: " +
							(result.file != null ? result.file.toString() : "no file")
							+ " delay: " + delay);
				} else {
					logger.info("Success with Id: " +
							result.runner.getSpecialId() + " on Final file: " +
							(result.file != null ? result.file.toString() : "no file")
							+ " delay: " + delay);
				}
				if (nolog) {
					// In case of success, delete the runner
					try {
						result.runner.delete();
					} catch (GoldenGateDatabaseException e) {
						logger.warn("Cannot apply nolog to " + result.runner.toString(), e);
					}
				}
			} else {
				if (result == null || result.runner == null) {
					logger.warn("Transfer in Error with no Id", future.getCause());
					networkTransaction.closeAll();
					System.exit(1);
				}
				if (result.runner.getErrorInfo() == ErrorCode.Warning) {
					logger.warn("Transfer in Warning with Id: " +
							result.runner.getSpecialId(), future.getCause());
					networkTransaction.closeAll();
					System.exit(result.code.ordinal());
				} else {
					logger.error("Transfer in Error with Id: " +
							result.runner.getSpecialId(), future.getCause());
					networkTransaction.closeAll();
					System.exit(result.code.ordinal());
				}
			}
		} finally {
			networkTransaction.closeAll();
		}

	}

}
