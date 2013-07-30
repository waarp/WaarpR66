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
import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.logging.InternalLoggerFactory;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbRule;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;

/**
 * Client to submit a transfer for multiple files to multiple hosts at once.<br>
 * Files will have to be separated by ','.<br>
 * Hosts will have to be separated by ','.<br>
 * <br>
 * For instance: -to host1,host2,host3 -file file1,file2 <br>
 * Will generate: <br>
 * -to host1 -file file1<br>
 * -to host1 -file file2<br>
 * -to host2 -file file1<br>
 * -to host2 -file file2<br>
 * -to host3 -file file1<br>
 * -to host3 -file file2<br>
 * <br>
 * <br>Extra option is -client which allows the filename resolution on remote (recv files) when using wildcards.<br>
 * 
 * @author Frederic Bregier
 * 
 */
public class MultipleSubmitTransfer extends SubmitTransfer {

	public MultipleSubmitTransfer(R66Future future, String remoteHost,
			String filename, String rulename, String fileinfo, boolean isMD5, int blocksize,
			long id,
			Timestamp starttime) {
		super(future, remoteHost, filename, rulename, fileinfo, isMD5, blocksize, id, starttime);
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
			logger = WaarpInternalLoggerFactory.getLogger(MultipleSubmitTransfer.class);
		}
		boolean submit = true;
		for (String string : args) {
			if (string.equalsIgnoreCase("-client")) {
				submit = false;
			}
		}
		if (!getParams(args, submit)) {
			logger.error("Wrong initialization");
			if (DbConstant.admin != null && DbConstant.admin.isConnected) {
				DbConstant.admin.close();
			}
			ChannelUtils.stopLogger();
			System.exit(1);
		}
		String [] localfilenames = localFilename.split(",");
		String [] rhosts = rhost.split(",");
		int error = 0;
		boolean inError = false;
		// first check if filenames contains wildcards
		DbRule dbrule = null;
		try {
			dbrule = new DbRule(DbConstant.admin.session, rule);
		} catch (WaarpDatabaseException e) {
			logger.error("Rule is not found: "+rule);
			ChannelUtils.stopLogger();
			System.exit(2);
		}
		NetworkTransaction networkTransaction = null;
		if (! submit) {
			Configuration.configuration.pipelineInit();
			networkTransaction = new NetworkTransaction();
		}
		try {
			List<String> files = null;
			if (dbrule.isSendMode()) {
				files = MultipleDirectTransfer.getLocalFiles(dbrule, localfilenames);
			} else if (submit) {
				files = new ArrayList<String>();
				for (String string : localfilenames) {
					files.add(string);
				}
			}
			for (String host : rhosts) {
				host = host.trim();
				if (host != null && ! host.isEmpty()) {
					if (! submit && dbrule.isRecvMode()) {
						files = MultipleDirectTransfer.getRemoteFiles(dbrule, localfilenames, host, networkTransaction);
					}
					for (String filename : files) {
						filename = filename.trim();
						if (filename != null && ! filename.isEmpty()) {
							R66Future future = new R66Future(true);
							MultipleSubmitTransfer transaction = new MultipleSubmitTransfer(future,
									host, filename, rule, fileInfo, ismd5, block, idt,
									ttimestart);
							transaction.run();
							future.awaitUninterruptibly();
							DbTaskRunner runner = future.getResult().runner;
							if (future.isSuccess()) {
								logger.warn("Prepare transfer in     SUCCESS     " + runner.toShortString() +
										"<REMOTE>" + rhost + "</REMOTE>");
							} else {
								if (runner != null) {
									logger.error("Prepare transfer in     FAILURE      " + runner.toShortString() +
											"<REMOTE>" + rhost + "</REMOTE>", future.getCause());
								} else {
									logger.error("Prepare transfer in     FAILURE      ", future.getCause());
								}
								error = future.getResult().code.ordinal();
								inError = true;
							}
						}
					}
				}
			}
			if (inError) {
				if (networkTransaction != null)  {
					networkTransaction.closeAll();
					networkTransaction = null;
				}
				DbConstant.admin.close();
				ChannelUtils.stopLogger();
				System.exit(error);
			}
			DbConstant.admin.close();
		} finally {
			if (networkTransaction != null)  {
				networkTransaction.closeAll();
			}
		}
	}

}
