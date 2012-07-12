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
package openr66.context.task;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.io.File;
import java.io.IOException;

import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.R66Session;
import openr66.context.task.localexec.LocalExecClient;
import openr66.protocol.configuration.Configuration;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;

/**
 * Execute an external command
 * 
 * 
 * @author Frederic Bregier
 * 
 */
public class ExecTask extends AbstractTask {

	/**
	 * Internal Logger
	 */
	private static final GgInternalLogger logger = GgInternalLoggerFactory
			.getLogger(ExecTask.class);

	/**
	 * @param argRule
	 * @param delay
	 * @param argTransfer
	 * @param session
	 */
	public ExecTask(String argRule, int delay, String argTransfer,
			R66Session session) {
		super(TaskType.EXEC, delay, argRule, argTransfer, session);
	}

	/*
	 * (non-Javadoc)
	 * @see openr66.context.task.AbstractTask#run()
	 */
	@Override
	public void run() {
		/*
		 * First apply all replacements and format to argRule from context and argTransfer. Will
		 * call exec (from first element of resulting string) with arguments as the following value
		 * from the replacements. Return 0 if OK, else 1 for a warning else as an error. No change
		 * should be done in the FILENAME
		 */
		logger.debug("Exec with " + argRule + ":" + argTransfer + " and {}",
				session);
		String finalname = argRule;
		finalname = getReplacedValue(finalname, argTransfer.split(" "));
		// Check if the execution will be done through LocalExec daemon
		if (Configuration.configuration.useLocalExec && useLocalExec) {
			LocalExecClient localExecClient = new LocalExecClient();
			if (localExecClient.connect()) {
				localExecClient
						.runOneCommand(finalname, delay, waitForValidation, futureCompletion);
				localExecClient.disconnect();
				return;
			}// else continue
		}
		// Execution is done internally
		String[] args = finalname.split(" ");
		File exec = new File(args[0]);
		if (exec.isAbsolute()) {
			if (!exec.canExecute()) {
				logger.error("Exec command is not executable: " + finalname);
				R66Result result = new R66Result(session, false,
						ErrorCode.CommandNotFound, session.getRunner());
				futureCompletion.setResult(result);
				futureCompletion.cancel();
				return;
			}
		}
		CommandLine commandLine = new CommandLine(args[0]);
		for (int i = 1; i < args.length; i++) {
			commandLine.addArgument(args[i]);
		}
		DefaultExecutor defaultExecutor = new DefaultExecutor();
		PumpStreamHandler pumpStreamHandler = new PumpStreamHandler(null, null);
		defaultExecutor.setStreamHandler(pumpStreamHandler);
		int[] correctValues = {
				0, 1 };
		defaultExecutor.setExitValues(correctValues);
		ExecuteWatchdog watchdog = null;
		if (delay > 0 && waitForValidation) {
			watchdog = new ExecuteWatchdog(delay);
			defaultExecutor.setWatchdog(watchdog);
		}
		if (!waitForValidation) {
			// Do not wait for validation
			futureCompletion.setSuccess();
			logger.info("Exec will start but no WAIT with {}", commandLine);
		}
		int status = -1;
		try {
			status = defaultExecutor.execute(commandLine);
		} catch (ExecuteException e) {
			if (e.getExitValue() == -559038737) {
				// Cannot run immediately so retry once
				try {
					Thread.sleep(Configuration.RETRYINMS);
				} catch (InterruptedException e1) {
				}
				try {
					status = defaultExecutor.execute(commandLine);
				} catch (ExecuteException e1) {
					pumpStreamHandler.stop();
					logger.error("Exception: " + e.getMessage() +
							" Exec in error with " + commandLine.toString());
					if (waitForValidation) {
						futureCompletion.setFailure(e);
					}
					return;
				} catch (IOException e1) {
					pumpStreamHandler.stop();
					logger.error("Exception: " + e.getMessage() +
							" Exec in error with " + commandLine.toString());
					if (waitForValidation) {
						futureCompletion.setFailure(e);
					}
					return;
				}
			} else {
				pumpStreamHandler.stop();
				logger.error("Exception: " + e.getMessage() +
						" Exec in error with " + commandLine.toString());
				if (waitForValidation) {
					futureCompletion.setFailure(e);
				}
				return;
			}
		} catch (IOException e) {
			pumpStreamHandler.stop();
			logger.error("Exception: " + e.getMessage() +
					" Exec in error with " + commandLine.toString());
			if (waitForValidation) {
				futureCompletion.setFailure(e);
			}
			return;
		}
		pumpStreamHandler.stop();
		if (defaultExecutor.isFailure(status) && watchdog != null &&
				watchdog.killedProcess()) {
			// kill by the watchdoc (time out)
			logger.error("Exec is in Time Out");
			status = -1;
		}
		if (status == 0) {
			if (waitForValidation) {
				futureCompletion.setSuccess();
			}
			logger.info("Exec OK with {}", commandLine);
		} else if (status == 1) {
			logger.warn("Exec in warning with " + commandLine.toString());
			if (waitForValidation) {
				futureCompletion.setSuccess();
			}
		} else {
			logger.error("Status: " + status + " Exec in error with " +
					commandLine.toString());
			if (waitForValidation) {
				futureCompletion.cancel();
			}
		}
	}

}
