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
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbRule;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.Messages;
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
	public int errorMultiple = 0;
	public int doneMultiple = 0;
	protected boolean submit = false;
	protected NetworkTransaction networkTransaction = null;
	public List<R66Result> resultsSuccess = new ArrayList<R66Result>();
	public List<R66Future> futuresError = new ArrayList<R66Future>();

	public MultipleSubmitTransfer(R66Future future, String remoteHost,
			String filename, String rulename, String fileinfo, boolean isMD5, int blocksize,
			long id,
			Timestamp starttime, NetworkTransaction networkTransaction) {
		super(future, remoteHost, filename, rulename, fileinfo, isMD5, blocksize, id, starttime);
		this.networkTransaction = networkTransaction;
	}

	
	@Override
	public void run() {
		String [] localfilenames = filename.split(",");
		String [] rhosts = remoteHost.split(",");
		R66Result resultError = null;

		// first check if filenames contains wildcards
		DbRule dbrule = null;
		try {
			dbrule = new DbRule(DbConstant.admin.session, rulename);
		} catch (WaarpDatabaseException e) {
			logger.error(Messages.getString("SubmitTransfer.2")+rule); //$NON-NLS-1$
			ChannelUtils.stopLogger();
			System.exit(2);
		}
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
						SubmitTransfer transaction = new SubmitTransfer(future,
								host, filename, rule, fileInfo, ismd5, block, idt,
								ttimestart);
						transaction.run();
						future.awaitUninterruptibly();
						DbTaskRunner runner = future.getResult().runner;
						if (future.isSuccess()) {
							resultsSuccess.add(future.getResult());
							logger.warn(Messages.getString("SubmitTransfer.3")+Messages.getString("RequestInformation.Success") + runner.toShortString() + //$NON-NLS-1$
									"<REMOTE>" + rhost + "</REMOTE>");
							doneMultiple++;
						} else {
							futuresError.add(future);
							if (runner != null) {
								logger.error(Messages.getString("SubmitTransfer.3")+Messages.getString("RequestInformation.Failure") + runner.toShortString() + //$NON-NLS-1$
										"<REMOTE>" + rhost + "</REMOTE>", future.getCause());
							} else {
								logger.error(Messages.getString("SubmitTransfer.3")+Messages.getString("RequestInformation.Failure"), future.getCause()); //$NON-NLS-1$
							}
							errorMultiple++;
							resultError = future.getResult();
						}
					}
				}
			}
		}
		if (errorMultiple > 0) {
			if (resultError != null) {
				this.future.setResult(resultError);
			}
			this.future.cancel();
		} else {
			this.future.setSuccess();
		}
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
			logger.error(Messages.getString("Configuration.WrongInit")); //$NON-NLS-1$
			if (! Configuration.configuration.quietClient) {
				System.out.println(Messages.getString("Configuration.WrongInit")); //$NON-NLS-1$
			}
			if (DbConstant.admin != null && DbConstant.admin.isConnected) {
				DbConstant.admin.close();
			}
			ChannelUtils.stopLogger();
			System.exit(1);
		}
		NetworkTransaction networkTransaction = null;
		if (! submit) {
			Configuration.configuration.pipelineInit();
			networkTransaction = new NetworkTransaction();
		}
		try {
			R66Future future = new R66Future(true);
			MultipleSubmitTransfer transaction = new MultipleSubmitTransfer(future,
					rhost, localFilename, rule, fileInfo, ismd5, block, idt,
					ttimestart, networkTransaction);
			transaction.run();
			future.awaitUninterruptibly();
			if (future.isSuccess()) {
				logger.warn(Messages.getString("SubmitTransfer.3")+Messages.getString("RequestInformation.Success")+transaction.doneMultiple //$NON-NLS-1$
						+ Messages.getString("SubmitTransfer.Transfers")); //$NON-NLS-1$
				if (! Configuration.configuration.quietClient) {
					System.out.println(Messages.getString("SubmitTransfer.3")+Messages.getString("RequestInformation.Success")+"\n"+transaction.doneMultiple //$NON-NLS-1$
							+ "\n" +Messages.getString("SubmitTransfer.Transfers")); //$NON-NLS-1$
					for (R66Result result : transaction.resultsSuccess) {
						System.out.println(Messages.getString("SubmitTransfer.3")+Messages.getString("RequestInformation.Success") + "\n"+
								result.runner.toShortString() + //$NON-NLS-1$
								"<REMOTE>" + rhost + "</REMOTE>");
					}
				}
			} else {
				logger.error(Messages.getString("SubmitTransfer.14")+ //$NON-NLS-1$
						transaction.errorMultiple +
						" ok: "+ transaction.doneMultiple
						+ Messages.getString("SubmitTransfer.Transfers")); //$NON-NLS-1$
				if (! Configuration.configuration.quietClient) {
					System.out.println(Messages.getString("SubmitTransfer.14")+ //$NON-NLS-1$
							"\n"+transaction.errorMultiple +"\n" +
							" ok: "+ "\n"+transaction.doneMultiple
							+"\n" + Messages.getString("SubmitTransfer.Transfers")); //$NON-NLS-1$
					for (R66Future subfuture : transaction.futuresError) {
						R66Result result = subfuture.getResult();
						if (result.runner != null) {
							System.out.println(Messages.getString("SubmitTransfer.3")+Messages.getString("RequestInformation.Failure") + "\n"+
									result.runner.toShortString() + //$NON-NLS-1$
									"<REMOTE>" + rhost + "</REMOTE>" + "\n"+ future.getCause());
						} else {
							System.out.println(Messages.getString("SubmitTransfer.3")+Messages.getString("RequestInformation.Failure") +"\n"+
									future.getCause()); //$NON-NLS-1$
						}
					}
				}
				if (networkTransaction != null)  {
					networkTransaction.closeAll();
					networkTransaction = null;
				}
				DbConstant.admin.close();
				ChannelUtils.stopLogger();
				System.exit(transaction.errorMultiple);
			}
			DbConstant.admin.close();
		} finally {
			if (networkTransaction != null)  {
				networkTransaction.closeAll();
			}
		}
	}

}
