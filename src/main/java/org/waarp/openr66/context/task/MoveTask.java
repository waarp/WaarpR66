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
import org.waarp.openr66.context.filesystem.R66Dir;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;

/**
 * Move the file (without renaming it)
 * 
 * @author Frederic Bregier
 * 
 */
public class MoveTask extends AbstractTask {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(MoveTask.class);

	/**
	 * @param argRule
	 * @param delay
	 * @param argTransfer
	 * @param session
	 */
	public MoveTask(String argRule, int delay, String argTransfer,
			R66Session session) {
		super(TaskType.MOVE, delay, argRule, argTransfer, session);
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.context.task.AbstractTask#run()
	 */
	@Override
	public void run() {
		logger.info("Move with " + argRule + ":" + argTransfer + " and {}",
				session);
		boolean success = false;
		String directory = argRule;
		directory = getReplacedValue(directory, argTransfer.split(" "));
		String finalname = directory.split(" ")[0] + R66Dir.SEPARATOR +
				session.getFile().getBasename();
		try {
			success = session.getFile().renameTo(finalname, true);
		} catch (CommandAbstractException e) {
			logger.error("Move with " + argRule + ":" + argTransfer + " to " + finalname + " and " +
					session, e);
			futureCompletion.setFailure(new OpenR66ProtocolSystemException(e));
			return;
		}
		if (success) {
			session.getRunner().setFileMoved(finalname, success);
			futureCompletion.setSuccess();
		} else {
			logger.error("Cannot Move with " + argRule + ":" + argTransfer  + " to " + finalname +
					" and " + session);
			futureCompletion.setFailure(new OpenR66ProtocolSystemException(
					"Cannot move file"));
		}
	}

}
