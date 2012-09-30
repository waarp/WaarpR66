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

import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.protocol.configuration.Configuration;

/**
 * This class is for sending a SNMP trap/info (according to snmp configuration):<br>
 * - if delay is 0, only a warning will be send with the message accordingly<br>
 * - if delay is 1, a notification with the current task and the current message will be send<br>
 * <br>
 * 
 * @author Frederic Bregier
 * 
 */
public class SnmpTask extends AbstractTask {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(SnmpTask.class);
	/**
	 * @param argRule
	 * @param delay
	 * @param argTransfer
	 * @param session
	 */
	public SnmpTask(String argRule, int delay, String argTransfer,
			R66Session session) {
		super(TaskType.SNMP, delay, argRule, argTransfer, session);
	}

	@Override
	public void run() {
		if (Configuration.configuration.r66Mib == null) {
			logger.warn("SNMP support is not active");
			futureCompletion.setSuccess();
			return;
		}
		String finalValue = argRule;
		finalValue = getReplacedValue(finalValue, argTransfer.split(" "));
		switch (delay) {
			case 0:
				Configuration.configuration.r66Mib.notifyWarning(
						finalValue, "TransferId:" + this.session.getRunner().getSpecialId());
				break;
			case 1:
				Configuration.configuration.r66Mib.notifyInternalTask(
						finalValue, this.session.getRunner());
				break;
			default:
		}
		futureCompletion.setSuccess();
	}

}
