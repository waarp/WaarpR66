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
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;

/**
 * Move and Rename the current file
 * 
 * @author Frederic Bregier
 * 
 */
public class MoveRenameTask extends AbstractTask {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(MoveRenameTask.class);

	/**
	 * @param argRule
	 * @param delay
	 * @param argTransfer
	 * @param session
	 */
	public MoveRenameTask(String argRule, int delay, String argTransfer,
			R66Session session) {
		super(TaskType.MOVERENAME, delay, argRule, argTransfer, session);
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.context.task.AbstractTask#run()
	 */
	@Override
	public void run() {
		boolean success = false;
		String finalname = argRule;
		finalname = getReplacedValue(finalname, argTransfer.split(" ")).split(" ")[0].replace('\\', '/');
		logger.debug("Move and Rename to " + finalname + " with " + argRule +
				":" + argTransfer + " and {}", session);
		try {
			success = session.getFile().renameTo(finalname, true);
		} catch (CommandAbstractException e) {
			logger.error("Move and Rename to " + finalname + " with " +
					argRule + ":" + argTransfer + " and " + session, e);
			futureCompletion.setFailure(new OpenR66ProtocolSystemException(e));
			return;
		}
		if (success) {
			session.getRunner().setFileMoved(finalname, success);
			futureCompletion.setSuccess();
		} else {
			logger.error("Cannot Move and Rename to " + finalname + " with " +
					argRule + ":" + argTransfer + " and " + session);
			futureCompletion.setFailure(new OpenR66ProtocolSystemException(
					"Cannot move file"));
		}
	}

}
