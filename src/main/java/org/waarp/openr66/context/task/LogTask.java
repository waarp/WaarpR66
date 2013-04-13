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
package org.waarp.openr66.context.task;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.jboss.netty.logging.InternalLogLevel;

import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Session;

/**
 * This class is for logging or write to an external file some info:<br>
 * - if delay is 0, no echo at all will be done<br>
 * - if delay is 1, will echo some information in the normal log<br>
 * - if delay is 2, will echo some information in the file (last deduced argument will be the full
 * path for the file output)<br>
 * - if delay is 3, will echo both in the normal log and in the file (last deduced argument will be
 * the full path for the file output)<br>
 * <br>
 * If first word for logging is one of debug, info, warn, error, then the corresponding
 * level of log will be used.
 * 
 * @author Frederic Bregier
 * 
 */
public class LogTask extends AbstractTask {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(LogTask.class);

	/**
	 * @param argRule
	 * @param delay
	 * @param argTransfer
	 * @param session
	 */
	public LogTask(String argRule, int delay, String argTransfer,
			R66Session session) {
		super(TaskType.LOG, delay, argRule, argTransfer, session);
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.context.task.AbstractTask#run()
	 */
	@Override
	public void run() {
		String finalValue = argRule;
		finalValue = getReplacedValue(finalValue, argTransfer.split(" "));
		String tempValue = finalValue.toUpperCase();
		InternalLogLevel finalLevel = InternalLogLevel.WARN;
		for (InternalLogLevel level : InternalLogLevel.values()) {
			if (tempValue.startsWith(level.name())) {
				finalLevel = level;
				break;
			}
		}
		switch (delay) {
			case 0:
				break;
			case 1:
				logger.log(finalLevel, finalValue + "\n    " + session.toString());
				break;
			case 3:
				logger.log(finalLevel, finalValue + "\n    " + session.toString());
			case 2:
				String[] args = finalValue.split(" ");
				String filename = args[args.length - 1];
				File file = new File(filename);
				if (file.getParentFile() == null ||
						(file.exists() && !file.canWrite())) {
					// File cannot be written so revert to log
					session.getRunner().setErrorExecutionStatus(ErrorCode.Warning);
					if (delay == 2) {
						logger.log(finalLevel, finalValue + "\n    " + session.toString());
					}
					futureCompletion.setSuccess();
					return;
				}
				FileOutputStream outputStream = null;
				try {
					outputStream = new FileOutputStream(file, true);
				} catch (FileNotFoundException e) {
					// File cannot be written so revert to log
					session.getRunner().setErrorExecutionStatus(ErrorCode.Warning);
					if (delay == 2) {
						logger.log(finalLevel, finalValue + "\n    " + session.toString());
					}
					futureCompletion.setSuccess();
					return;
				}
				try {
					int len = args.length -1;
					for (int i = 0; i < len; i++) {
						outputStream.write(args[i].getBytes());
						outputStream.write(' ');
					}
					outputStream.write('\n');
				} catch (IOException e) {
					// File cannot be written so revert to log
					try {
						outputStream.close();
					} catch (IOException e1) {
					}
					file.delete();
					session.getRunner().setErrorExecutionStatus(ErrorCode.Warning);
					if (delay == 2) {
						logger.log(finalLevel, finalValue + "\n    " + session.toString());
					}
					futureCompletion.setSuccess();
					return;
				}
				try {
					outputStream.close();
				} catch (IOException e) {
				}
				break;
			default:
		}
		futureCompletion.setSuccess();
	}

}
