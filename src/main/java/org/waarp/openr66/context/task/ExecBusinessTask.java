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
import org.waarp.openr66.context.task.AbstractExecJavaTask;
import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.packet.BusinessRequestPacket;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;

/**
 * Example of Java Task for ExecJava
 * 
 * @author Frederic Bregier
 * 
 */
public class ExecBusinessTask extends AbstractExecJavaTask {

	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(ExecBusinessTask.class);

	@Override
	public void run() {
		if (callFromBusiness) {
			// Business Request to validate?
			if (isToValidate) {
				String operation = args[1];
				
				String argRule = "";
				for (int i = 2; i < args.length - 3; i++) {
					argRule += args[i];
					if (i < args.length - 4) {
						argRule += " ";
					}
				}
				int newdelay = 0;
				try {
					newdelay = Integer.parseInt(args[args.length-3]);
				} catch (NumberFormatException e) {
					newdelay = 0;
					argRule += args[args.length-3]; 
				}
				try {
					AbstractTask task = TaskType.getTaskFromIdForBusiness(operation, argRule, newdelay, this.session);
					if (task != null) {
						task.run();
						try {
							task.getFutureCompletion().await();
						} catch (InterruptedException e) {
						}
						R66Future future = task.getFutureCompletion();
						if ((!future.isDone()) || future.isFailed()) {
							invalid();
							return;
						}
					} else {
						logger.error("ExecBusiness in error, Task invalid: "+operation);
						invalid();
						return;
					}
				} catch (OpenR66RunnerErrorException e1) {
					logger.error("ExecBusiness in error: "+e1.toString());
					invalid();
					return;
				}
				BusinessRequestPacket packet =
						new BusinessRequestPacket(this.getClass().getName() +" execution ok", 0);
				validate(packet);
				try {
					ChannelUtils.writeAbstractLocalPacket(session.getLocalChannelReference(),
							packet, true);
				} catch (OpenR66ProtocolPacketException e) {
				}
				this.status = 0;
				return;
			}
			finalValidate("Validated");
			return;
		} else {
			// Rule EXECJAVA based should be used instead
			this.status = 2;
			args[args.length-1] = "EXECJAVA should be used instead";
		}
	}
}
