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
package org.waarp.openr66.client;

import java.sql.Timestamp;

import org.jboss.netty.logging.InternalLoggerFactory;
import org.waarp.common.database.data.AbstractDbData;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.exception.OpenR66DatabaseGlobalException;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;

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
			logger = WaarpInternalLoggerFactory.getLogger(SubmitTransfer.class);
		}
		long srcId = id;
		DbTaskRunner taskRunner = this.initRequest();
		if (taskRunner == null) {
			logger.debug("Cannot prepare task");
			R66Result result = new R66Result(new OpenR66DatabaseGlobalException(), null, true,
					ErrorCode.Internal, taskRunner);
			future.setResult(result);
			future.setFailure(result.exception);
			return;
		}
		if (srcId != DbConstant.ILLEGALVALUE) {
			// Resubmit call, some checks are needed
			if (! taskRunner.restart(true)) {
				// cannot be done from there => must be done through IHM
				logger.debug("Cannot prepare task from there. IHM must be used");
				R66Result result = new R66Result(
						new OpenR66DatabaseGlobalException("Cannot prepare task from there. IHM must be used"), 
						null, true,
						ErrorCode.Internal, taskRunner);
				future.setResult(result);
				future.setFailure(result.exception);
				return;
			}
		} else {
			taskRunner.changeUpdatedInfo(AbstractDbData.UpdatedInfo.TOSUBMIT);
		}
		if (!taskRunner.forceSaveStatus()) {
			logger.debug("Cannot prepare task");
			R66Result result = new R66Result(new OpenR66DatabaseGlobalException("Cannot prepare Task"), null, true,
					ErrorCode.Internal, taskRunner);
			future.setResult(result);
			future.setFailure(result.exception);
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
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(SubmitTransfer.class);
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
				rhost, localFilename, rule, fileInfo, ismd5, block, idt,
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
