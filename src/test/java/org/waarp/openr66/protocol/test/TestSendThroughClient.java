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
package org.waarp.openr66.protocol.test;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.exception.FileEndOfTransferException;
import org.waarp.common.exception.FileTransferException;
import org.waarp.common.file.DataBlock;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.client.SendThroughClient;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.filesystem.R66File;
import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.R66Future;

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
			ChannelFuture future1 = null, future2 = null;
			if (block != null) {
				future1 = this.writeWhenPossible(block);
			}
			// While not last block
			while (block != null && !block.isEOF()) {
				try {
					block = r66file.readDataBlock();
				} catch (FileEndOfTransferException e) {
					// Wait for last write
					retrieveDone = true;
					try {
						future1.await();
					} catch (InterruptedException e1) {
					}
					return future1.isSuccess();
				}
				future2 = this.writeWhenPossible(block);
				try {
					future1.await();
				} catch (InterruptedException e) {
				}
				if (!future1.isSuccess()) {
					return false;
				}
				future1 = future2;
			}
			// Wait for last write
			if (future1 != null) {
				try {
					future1.await();
				} catch (InterruptedException e) {
				}
				return future1.isSuccess();
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
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(
				null));
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(TestSendThroughClient.class);
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
					logger.warn("Success with Id: " +
							result.runner.getSpecialId() + " on Final file: " +
							(result.file != null ? result.file.toString() : "no file")
							+ " delay: " + delay);
				}
				if (nolog) {
					// In case of success, delete the runner
					try {
						result.runner.delete();
					} catch (WaarpDatabaseException e) {
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
