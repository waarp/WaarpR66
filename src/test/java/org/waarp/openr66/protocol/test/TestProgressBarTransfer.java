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

import org.jboss.netty.logging.InternalLoggerFactory;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.client.ProgressBarTransfer;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;

/**
 * @author Frederic Bregier
 * 
 */
public class TestProgressBarTransfer extends ProgressBarTransfer {

	/**
	 * @param future
	 * @param remoteHost
	 * @param filename
	 * @param rulename
	 * @param fileinfo
	 * @param isMD5
	 * @param blocksize
	 * @param id
	 * @param networkTransaction
	 * @param callbackdelay
	 */
	public TestProgressBarTransfer(R66Future future, String remoteHost,
			String filename, String rulename, String fileinfo, boolean isMD5,
			int blocksize, long id, NetworkTransaction networkTransaction,
			long callbackdelay) {
		super(future, remoteHost, filename, rulename, fileinfo, isMD5,
				blocksize, id, networkTransaction, callbackdelay);
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.client.ProgressBarTransfer#callBack(int, int)
	 */
	@Override
	public void callBack(int currentBlock, int blocksize) {
		if (filesize == 0) {
			System.err.println("Block: " + currentBlock + " BSize: " + blocksize);
		} else {
			System.err.println("Block: " + currentBlock + " BSize: " + blocksize + " on " +
					(int) (Math.ceil(((double) filesize / (double) blocksize))));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.client.ProgressBarTransfer#lastCallBack(boolean, int, int)
	 */
	@Override
	public void lastCallBack(boolean success, int currentBlock, int blocksize) {
		if (filesize == 0) {
			System.err.println("Status: " + success + " Block: " + currentBlock + " BSize: "
					+ blocksize);
		} else {
			System.err.println("Status: " + success + " Block: " + currentBlock + " BSize: "
					+ blocksize +
					" Size=" + filesize);
		}
	}

	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(
				null));
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(ProgressBarTransfer.class);
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
			TestProgressBarTransfer transaction = new TestProgressBarTransfer(future,
					rhost, localFilename, rule, fileInfo, ismd5, block, idt,
					networkTransaction, 100);
			transaction.run();
			future.awaitUninterruptibly();
			long time2 = System.currentTimeMillis();
			long delay = time2 - time1;
			R66Result result = future.getResult();
			if (future.isSuccess()) {
				if (result.runner.getErrorInfo() == ErrorCode.Warning) {
					logger.warn("Transfer in status: WARNED     "
							+ result.runner.toShortString()
							+
							"     <REMOTE>"
							+ rhost
							+ "</REMOTE>"
							+
							"     <FILEFINAL>"
							+
							(result.file != null ? result.file.toString() + "</FILEFINAL>"
									: "no file")
							+ "     delay: " + delay);
				} else {
					logger.info("Transfer in status: SUCCESS     "
							+ result.runner.toShortString()
							+
							"     <REMOTE>"
							+ rhost
							+ "</REMOTE>"
							+
							"     <FILEFINAL>"
							+
							(result.file != null ? result.file.toString() + "</FILEFINAL>"
									: "no file")
							+ "     delay: " + delay);
				}
				if (nolog || result.runner.shallIgnoreSave()) {
					// In case of success, delete the runner
					try {
						result.runner.delete();
					} catch (WaarpDatabaseException e) {
						logger.warn("Cannot apply nolog to     " + result.runner.toShortString(),
								e);
					}
				}
			} else {
				if (result == null || result.runner == null) {
					logger.error("Transfer in     FAILURE with no Id", future.getCause());
					networkTransaction.closeAll();
					System.exit(ErrorCode.Unknown.ordinal());
				}
				if (result.runner.getErrorInfo() == ErrorCode.Warning) {
					logger.warn("Transfer is     WARNED     " + result.runner.toShortString() +
							"     <REMOTE>" + rhost + "</REMOTE>", future.getCause());
					networkTransaction.closeAll();
					System.exit(result.code.ordinal());
				} else {
					logger.error("Transfer in     FAILURE     " + result.runner.toShortString() +
							"     <REMOTE>" + rhost + "</REMOTE>", future.getCause());
					networkTransaction.closeAll();
					System.exit(result.code.ordinal());
				}
			}
		} finally {
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
