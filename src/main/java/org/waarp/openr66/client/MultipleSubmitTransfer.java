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
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbTaskRunner;
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
		if (!getParams(args, true)) {
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
		for (String host : rhosts) {
			host = host.trim();
			if (host != null && host.length() > 0) {
				for (String filename : localfilenames) {
					filename = filename.trim();
					if (filename != null && filename.length() > 0) {
						R66Future future = new R66Future(true);
						MultipleSubmitTransfer transaction = new MultipleSubmitTransfer(future,
								host, filename, rule, fileInfo, ismd5, block, idt,
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
							error = future.getResult().code.ordinal();
							inError = true;
						}
					}
				}
			}
		}
		if (inError) {
			DbConstant.admin.close();
			ChannelUtils.stopLogger();
			System.exit(error);
		}
		DbConstant.admin.close();
	}

}
