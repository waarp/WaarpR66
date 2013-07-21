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

import org.jboss.netty.logging.InternalLoggerFactory;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;

/**
 * Direct Transfer from a client with or without database connection to transfer for multiple files to multiple hosts at once.<br>
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
public class MultipleDirectTransfer extends DirectTransfer {
	public MultipleDirectTransfer(R66Future future, String remoteHost,
			String filename, String rulename, String fileinfo, boolean isMD5, int blocksize,
			long id,
			NetworkTransaction networkTransaction) {
		// no starttime since it is direct (blocking request, no delay)
		super(future, remoteHost, filename, rulename, fileinfo, isMD5, blocksize, id, networkTransaction);
	}

	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(MultipleDirectTransfer.class);
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

		Configuration.configuration.pipelineInit();
		NetworkTransaction networkTransaction = new NetworkTransaction();
		try {
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
							logger.warn("Launch transfer to "+host+" with file "+filename);
							R66Future future = new R66Future(true);
							MultipleDirectTransfer transaction = new MultipleDirectTransfer(future,
									host, filename, rule, fileInfo, ismd5, block, idt,
									networkTransaction);
							logger.debug("rhost: "+rhost+":"+transaction.remoteHost);
							transaction.run();
							future.awaitUninterruptibly();
							long time2 = System.currentTimeMillis();
							logger.debug("finish transfer: " + future.isSuccess());
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
								if (nolog) {
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
									inError = true;
									error = ErrorCode.Unknown.ordinal();
								}
								if (result.runner.getErrorInfo() == ErrorCode.Warning) {
									logger.warn("Transfer is     WARNED     " + result.runner.toShortString() +
											"     <REMOTE>" + rhost + "</REMOTE>", future.getCause());
									inError = true;
									error = result.code.ordinal();
								} else {
									logger.error("Transfer in     FAILURE     " + result.runner.toShortString() +
											"     <REMOTE>" + rhost + "</REMOTE>", future.getCause());
									inError = true;
									error = result.code.ordinal();
								}
							}
						}
					}
				}
			}
			if (inError) {
				networkTransaction.closeAll();
				System.exit(error);
			}
		} catch (Exception e) {
			logger.debug("exc", e);
		} finally {
			networkTransaction.closeAll();
			System.exit(0);
		}
	}

}
