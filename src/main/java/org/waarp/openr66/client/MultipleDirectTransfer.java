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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.logging.InternalLoggerFactory;
import org.waarp.common.command.exception.CommandAbstractException;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.context.filesystem.R66Dir;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbRule;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.Messages;
import org.waarp.openr66.protocol.localhandler.packet.InformationPacket;
import org.waarp.openr66.protocol.localhandler.packet.ValidPacket;
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
	public int errorMultiple = 0;
	public int doneMultiple = 0;
	
	public MultipleDirectTransfer(R66Future future, String remoteHost,
			String filename, String rulename, String fileinfo, boolean isMD5, int blocksize,
			long id,
			NetworkTransaction networkTransaction) {
		// no starttime since it is direct (blocking request, no delay)
		super(future, remoteHost, filename, rulename, fileinfo, isMD5, blocksize, id, networkTransaction);
	}
	
	public static List<String> getRemoteFiles(DbRule dbrule, String []localfilenames, String requested, NetworkTransaction networkTransaction) {
		List<String> files = new ArrayList<String>();
		for (String filename : localfilenames) {
			if (!(filename.contains("*") || filename.contains("?") || filename.contains("~"))) {
				files.add(filename);
			} else {
				// remote query
				R66Future futureInfo = new R66Future(true);
				logger.info(Messages.getString("Transfer.3")+filename+ " to "+requested); //$NON-NLS-1$
				RequestInformation info = new RequestInformation(futureInfo, requested, rule, filename,
						(byte) InformationPacket.ASKENUM.ASKLIST.ordinal(), -1, false, networkTransaction);
				info.run();
				futureInfo.awaitUninterruptibly();
				if (futureInfo.isSuccess()) {
					ValidPacket valid = (ValidPacket) futureInfo.getResult().other;
					if (valid != null) {
						String line = valid.getSheader();
						String []lines = line.split("\n");
						for (String string : lines) {
							File tmpFile = new File(string);
							files.add(tmpFile.getName());
						}
					}
				} else {
					logger.error(Messages.getString("Transfer.6")+filename+ " to "+requested + ": "+ futureInfo.getCause().getMessage()); //$NON-NLS-1$
				}
			}
		}
		return files;
	}


	public static List<String> getLocalFiles(DbRule dbrule, String []localfilenames) {
		List<String> files = new ArrayList<String>();
		R66Session session = new R66Session();
		session.getAuth().specialNoSessionAuth(true, Configuration.configuration.HOST_ID);
		R66Dir dir = new R66Dir(session);
		try {
			dir.changeDirectory(dbrule.getSendPath());
		} catch (CommandAbstractException e) {
		}
		if (localfilenames != null) {
			for (String filename : localfilenames) {
				if (!(filename.contains("*") || filename.contains("?") || filename.contains("~"))) {
					files.add(filename);
				} else {
					// local: must check
					logger.info("Local Ask for "+filename+" from "+dir.getFullPath());
					List<String> list;
					try {
						list = dir.list(filename);
						if (list != null) {
							files.addAll(list);
						}
					} catch (CommandAbstractException e) {
						logger.warn(Messages.getString("Transfer.14")+filename + " : "+e.getMessage()); //$NON-NLS-1$
					}
				}
			}
		}
		return files;
	}
	

	@Override
	public void run() {
		String [] localfilenames = filename.split(",");
		String [] rhosts = remoteHost.split(",");
		boolean inError = false;
		R66Result resultError = null;
		// first check if filenames contains wildcards
		DbRule dbrule = null;
		try {
			dbrule = new DbRule(DbConstant.admin.session, rulename);
		} catch (WaarpDatabaseException e1) {
			logger.error(Messages.getString("Transfer.18"), e1); //$NON-NLS-1$
			this.future.setFailure(e1);
			return;
		}
		List<String> files = null;
		if (dbrule.isSendMode()) {
			files = getLocalFiles(dbrule, localfilenames);
		}
		for (String host : rhosts) {
			host = host.trim();
			if (host != null && ! host.isEmpty()) {
				if (dbrule.isRecvMode()) {
					files = getRemoteFiles(dbrule, localfilenames, host, networkTransaction);
				}
				for (String filename : files) {
					filename = filename.trim();
					if (filename != null && ! filename.isEmpty()) {
						logger.info("Launch transfer to "+host+" with file "+filename);
						long time1 = System.currentTimeMillis();
						R66Future future = new R66Future(true);
						DirectTransfer transaction = new DirectTransfer(future,
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
							doneMultiple++;
							if (result.runner.getErrorInfo() == ErrorCode.Warning) {
								logger.warn(Messages.getString("Transfer.Status")+Messages.getString("RequestInformation.Warned")
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
										+ Messages.getString("R66Environment.13") + delay); //$NON-NLS-1$
							} else {
								logger.warn(Messages.getString("Transfer.Status")+Messages.getString("RequestInformation.Success")
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
										+ Messages.getString("R66Environment.13") + delay); //$NON-NLS-1$
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
							errorMultiple++;
							if (result == null || result.runner == null) {
								logger.error(Messages.getString("Transfer.FailedNoId"), future.getCause());
								inError = true;
							}
							if (result.runner.getErrorInfo() == ErrorCode.Warning) {
								logger.warn(Messages.getString("Transfer.Status")+Messages.getString("RequestInformation.Warned") + result.runner.toShortString() +
										"     <REMOTE>" + rhost + "</REMOTE>", future.getCause());
								inError = true;
								resultError = result;
							} else {
								logger.error(Messages.getString("Transfer.Status")+Messages.getString("RequestInformation.Failure") + result.runner.toShortString() +
										"     <REMOTE>" + rhost + "</REMOTE>", future.getCause());
								inError = true;
								resultError = result;
							}
						}
					}
				}
			}
		}
		if (inError) {
			if (resultError != null) {
				this.future.setResult(resultError);
			}
			this.future.cancel();
		} else {
			this.future.setSuccess();
		}
	}

	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(MultipleDirectTransfer.class);
		}
		if (!getParams(args, false)) {
			logger.error(Messages.getString("Configuration.WrongInit")); //$NON-NLS-1$
			if (DbConstant.admin != null && DbConstant.admin.isConnected) {
				DbConstant.admin.close();
			}
			ChannelUtils.stopLogger();
			System.exit(2);
		}

		Configuration.configuration.pipelineInit();
		NetworkTransaction networkTransaction = new NetworkTransaction();
		try {
			R66Future future = new R66Future(true);
			long time1 = System.currentTimeMillis();
			MultipleDirectTransfer multipleDirectTransfer =
					new MultipleDirectTransfer(future, rhost, localFilename, 
							rule, fileInfo, ismd5, block, idt,
							networkTransaction);
			multipleDirectTransfer.run();
			future.awaitUninterruptibly();
			long time2 = System.currentTimeMillis();
			logger.debug("finish all transfers: " + future.isSuccess());
			long delay = time2 - time1;
			if (future.isSuccess()) {
				logger.warn(Messages.getString("Transfer.48")+multipleDirectTransfer.doneMultiple //$NON-NLS-1$
						+ Messages.getString("Transfer.0") + delay); //$NON-NLS-1$
			} else {
				logger.error(Messages.getString("Transfer.50")+ //$NON-NLS-1$
						multipleDirectTransfer.errorMultiple +
						" ok: "+ multipleDirectTransfer.doneMultiple
						+ Messages.getString("Transfer.0") + delay); //$NON-NLS-1$
				networkTransaction.closeAll();
				System.exit(multipleDirectTransfer.errorMultiple);
			}
		} catch (Exception e) {
			logger.warn("exc", e);
		} finally {
			networkTransaction.closeAll();
			System.exit(0);
		}
	}

}
