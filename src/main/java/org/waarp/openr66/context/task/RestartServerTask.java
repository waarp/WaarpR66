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

import java.util.concurrent.TimeUnit;

import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.role.RoleDefault.ROLE;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66ShutdownHook;

/**
 * Command to Restart the R66 server (for instance after upgrade of jar sent by administrative operations, unzipped in the library directory)
 * 
 * 
 * @author Frederic Bregier
 * 
 */
public class RestartServerTask extends AbstractTask {

	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(RestartServerTask.class);

	/**
	 * @param argRule
	 * @param delay
	 * @param argTransfer
	 * @param session
	 */
	public RestartServerTask(String argRule, int delay, String argTransfer,
			R66Session session) {
		super(TaskType.RESTART, delay, argRule, argTransfer, session);
	}

	@Override
	public void run() {
		// check if allowed to do restart
		// SYSTEM authorization
		boolean isAdmin = session.getAuth().isValidRole(ROLE.SYSTEM);
		if (! isAdmin) {
			// not allowed
			logger.error("Shutdown order received but unallowed: " +
					session.getAuth().getUser());
			futureCompletion.setFailure(new OpenR66ProtocolSystemException("Shutdown order received but unallowed: " +
					session.getAuth().getUser()));
			return;
		}
		// now start the process
		logger.warn("Shutdown order received and going from: " +
				session.getAuth().getUser());
		R66ShutdownHook.setRestart(true);
		futureCompletion.setSuccess();
		Thread thread = new Thread(new ChannelUtils(), "R66 Shutdown Thread");
		thread.setDaemon(true);
		// give time for the task to finish correctly
		Configuration.configuration.launchInFixedDelay(thread, Configuration.WAITFORNETOP, TimeUnit.MILLISECONDS);
	}

}
