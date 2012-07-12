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

import goldengate.common.database.data.AbstractDbData.UpdatedInfo;
import goldengate.common.database.exception.GoldenGateDatabaseException;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.commander.ClientRunner;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolNotYetConnectionException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.R66Future;

/**
 * Through API Transfer from a client with or without database connection, and enabling access to
 * statistic of the transfer (unblocking transfer)
 * 
 * @author Frederic Bregier
 * 
 */
public abstract class ProgressBarTransfer extends AbstractTransfer {
	protected final NetworkTransaction networkTransaction;
	protected long INTERVALCALLBACK = 100;
	protected long filesize = 0;

	public ProgressBarTransfer(R66Future future, String remoteHost,
			String filename, String rulename, String fileinfo, boolean isMD5, int blocksize,
			long id,
			NetworkTransaction networkTransaction, long callbackdelay) {
		// no delay so starttime = null
		super(ProgressBarTransfer.class,
				future, filename, rulename, fileinfo, isMD5, remoteHost, blocksize, id, null);
		this.networkTransaction = networkTransaction;
		this.INTERVALCALLBACK = callbackdelay;
	}

	/**
	 * This function will be called every 100ms (or other fixed value in INTERVALCALLBACK). Note
	 * that final rank is unknown.
	 * 
	 * @param currentBlock
	 *            the current block rank (from 0 to n-1)
	 * @param blocksize
	 *            blocksize of 1 block
	 */
	abstract public void callBack(int currentBlock, int blocksize);

	/**
	 * This function will be called only once when the transfer is over
	 * 
	 * @param success
	 *            True if the transfer is successful
	 * @param currentBlock
	 * @param blocksize
	 */
	abstract public void lastCallBack(boolean success, int currentBlock, int blocksize);

	/**
	 * Prior to call this method, the pipeline and NetworkTransaction must have been initialized. It
	 * is the responsibility of the caller to finish all network resources.
	 */
	public void run() {
		if (logger == null) {
			logger = GgInternalLoggerFactory.getLogger(ProgressBarTransfer.class);
		}
		DbTaskRunner taskRunner = this.initRequest();
		ClientRunner runner = new ClientRunner(networkTransaction, taskRunner, future);
		OpenR66ProtocolNotYetConnectionException exc = null;
		for (int i = 0; i < Configuration.RETRYNB; i++) {
			try {
				LocalChannelReference localChannelReference = runner.initRequest();
				try {
					localChannelReference.getFutureValidRequest().await();
				} catch (InterruptedException e) {
				}
				if ((!localChannelReference.getFutureValidRequest().isSuccess()) &&
						(localChannelReference.getFutureValidRequest().getResult().code ==
						ErrorCode.ServerOverloaded)) {
					switch (taskRunner.getUpdatedInfo()) {
						case DONE:
						case INERROR:
						case INTERRUPTED:
							break;
						default:
							runner.changeUpdatedInfo(UpdatedInfo.INERROR,
									ErrorCode.ServerOverloaded);
					}
					// redo if possible
					if (runner.incrementTaskRunerTry(taskRunner, Configuration.RETRYNB)) {
						try {
							Thread.sleep(Configuration.configuration.constraintLimitHandler
									.getSleepTime());
						} catch (InterruptedException e) {
						}
						i--;
						continue;
					} else {
						throw new OpenR66ProtocolNotYetConnectionException(
								"End of retry on ServerOverloaded");
					}
				}
				this.filesize = future.filesize;
				while (!future.awaitUninterruptibly(INTERVALCALLBACK)) {
					if (future.isDone()) {
						break;
					}
					callBack(future.runner.getRank(), future.runner.getBlocksize());
				}
				runner.finishTransfer(false, localChannelReference);
				lastCallBack(future.isSuccess(),
						future.runner.getRank(), future.runner.getBlocksize());
				exc = null;
				break;
			} catch (OpenR66RunnerErrorException e) {
				logger.error("Cannot Transfer", e);
				future.setResult(new R66Result(e, null, true,
						ErrorCode.Internal, taskRunner));
				future.setFailure(e);
				lastCallBack(false, taskRunner.getRank(), taskRunner.getBlocksize());
				return;
			} catch (OpenR66ProtocolNoConnectionException e) {
				logger.error("Cannot Connect", e);
				future.setResult(new R66Result(e, null, true,
						ErrorCode.ConnectionImpossible, taskRunner));
				// since no connection : just forget it
				if (nolog) {
					try {
						taskRunner.delete();
					} catch (GoldenGateDatabaseException e1) {
					}
				}
				future.setFailure(e);
				lastCallBack(false, taskRunner.getRank(), taskRunner.getBlocksize());
				return;
			} catch (OpenR66ProtocolPacketException e) {
				logger.error("Bad Protocol", e);
				future.setResult(new R66Result(e, null, true,
						ErrorCode.TransferError, taskRunner));
				future.setFailure(e);
				lastCallBack(false, taskRunner.getRank(), taskRunner.getBlocksize());
				return;
			} catch (OpenR66ProtocolNotYetConnectionException e) {
				logger.debug("Not Yet Connected", e);
				exc = e;
				continue;
			}
		}
		if (exc != null) {
			taskRunner.setLocalChannelReference(new LocalChannelReference());
			logger.error("Cannot Connect", exc);
			future.setResult(new R66Result(exc, null, true,
					ErrorCode.ConnectionImpossible, taskRunner));
			lastCallBack(false, taskRunner.getRank(), taskRunner.getBlocksize());
			// since no connection : just forget it
			if (nolog) {
				try {
					taskRunner.delete();
				} catch (GoldenGateDatabaseException e1) {
				}
			}
			future.setFailure(exc);
			return;
		}
	}
}
