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
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;
import openr66.client.ProgressBarTransfer;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.database.DbConstant;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.ChannelUtils;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

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
	 * @see openr66.client.ProgressBarTransfer#callBack(int, int)
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
	 * @see openr66.client.ProgressBarTransfer#lastCallBack(boolean, int, int)
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
		InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
				Level.WARN));
		if (logger == null) {
			logger = GgInternalLoggerFactory.getLogger(ProgressBarTransfer.class);
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
