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

import org.waarp.common.command.exception.CommandAbstractException;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;

/**
 * Delete the file. The current file is no more valid.<br>
 * No arguments are taken into account.
 * 
 * @author Frederic Bregier
 * 
 */
public class DeleteTask extends AbstractTask {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(DeleteTask.class);

	/**
	 * @param argRule
	 * @param delay
	 * @param argTransfer
	 * @param session
	 */
	public DeleteTask(String argRule, int delay, String argTransfer,
			R66Session session) {
		super(TaskType.DELETE, delay, argRule, argTransfer, session);
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.context.task.AbstractTask#run()
	 */
	@Override
	public void run() {
		logger.info("Delete file from session {}",
				session);
		try {
			if (!session.getFile().delete()) {
				logger.debug("CANNOT Delete file {} from session {}", session.getFile().getFile(),
						session);
				R66Result result = new R66Result(session, false,
						ErrorCode.FileNotFound, session.getRunner());
				futureCompletion.setResult(result);
				futureCompletion.setFailure(new OpenR66ProtocolSystemException("File not deleted: "+session.getFile().getFile()));
				return;
			}
		} catch (CommandAbstractException e1) {
			logger.debug("CANNOT Delete file from session {}",
					session, e1);
			R66Result result = new R66Result(session, false,
					ErrorCode.FileNotFound, session.getRunner());
			futureCompletion.setResult(result);
			futureCompletion.setFailure(new OpenR66ProtocolSystemException(e1));
			return;
		}
		futureCompletion.setSuccess();
	}

}
