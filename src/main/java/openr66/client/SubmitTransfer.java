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

import goldengate.common.database.data.AbstractDbData;
import goldengate.common.database.exception.GoldenGateDatabaseException;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;

import java.sql.Timestamp;

import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.database.DbConstant;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.exception.OpenR66DatabaseGlobalException;
import openr66.protocol.utils.ChannelUtils;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.logging.InternalLoggerFactory;

/**
 * Client to submit a transfer
 * 
 * @author Frederic Bregier
 * 
 */
public class SubmitTransfer extends AbstractTransfer {

	public SubmitTransfer(R66Future future, String remoteHost,
			String filename, String rulename, String fileinfo, boolean isMD5, int blocksize,
			long id,
			Timestamp starttime) {
		super(SubmitTransfer.class,
				future, filename, rulename, fileinfo, isMD5, remoteHost, blocksize, id, starttime);
	}

	public void run() {
		if (logger == null) {
			logger = GgInternalLoggerFactory.getLogger(SubmitTransfer.class);
		}
		DbTaskRunner taskRunner = this.initRequest();
		taskRunner.changeUpdatedInfo(AbstractDbData.UpdatedInfo.TOSUBMIT);
		try {
			taskRunner.update();
		} catch (GoldenGateDatabaseException e) {
			logger.debug("Cannot prepare task", e);
			R66Result result = new R66Result(new OpenR66DatabaseGlobalException(e), null, true,
					ErrorCode.Internal, taskRunner);
			future.setResult(result);
			future.setFailure(e);
			return;
		}
		R66Result result = new R66Result(null, false, ErrorCode.InitOk, taskRunner);
		future.setResult(result);
		future.setSuccess();
	}

	/**
	 * 
	 * @param args
	 *            configuration file, the remoteHost Id, the file to transfer, the rule, file
	 *            transfer information as arguments and optionally isMD5=1 for true or 0 for
	 *            false(default) and the blocksize if different than default
	 */
	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(null));
		if (logger == null) {
			logger = GgInternalLoggerFactory.getLogger(SubmitTransfer.class);
		}
		if (!getParams(args, true)) {
			logger.error("Wrong initialization");
			if (DbConstant.admin != null && DbConstant.admin.isConnected) {
				DbConstant.admin.close();
			}
			ChannelUtils.stopLogger();
			System.exit(1);
		}
		R66Future future = new R66Future(true);
		SubmitTransfer transaction = new SubmitTransfer(future,
				rhost, localFilename, rule, fileInfo, ismd5, block, DbConstant.ILLEGALVALUE,
				ttimestart);
		transaction.run();
		future.awaitUninterruptibly();
		DbTaskRunner runner = future.getResult().runner;
		if (future.isSuccess()) {
			logger.warn("Prepare transfer in\n    SUCCESS\n    " + runner.toShortString() +
					"<REMOTE>" + rhost + "</REMOTE>");
		} else {
			if (runner != null) {
				logger.error("Prepare transfer in\n    FAILURE\n     " + runner.toShortString() +
						"<REMOTE>" + rhost + "</REMOTE>", future.getCause());
			} else {
				logger.error("Prepare transfer in\n    FAILURE\n     ", future.getCause());
			}
			DbConstant.admin.close();
			ChannelUtils.stopLogger();
			System.exit(future.getResult().code.ordinal());
		}
		DbConstant.admin.close();
	}

}
