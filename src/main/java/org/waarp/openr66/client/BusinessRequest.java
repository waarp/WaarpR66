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


import org.jboss.netty.logging.InternalLoggerFactory;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.client.AbstractBusinessRequest;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.localhandler.packet.BusinessRequestPacket;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;

/**
 * class for direct Business Request call
 * 
 * @author Frederic Bregier
 * 
 */
public class BusinessRequest extends AbstractBusinessRequest {
	/**
	 * Internal Logger
	 */
	private static WaarpInternalLogger logger;
	/**
	 * Default class
	 */
	public static final String DEFAULT_CLASS = "org.waarp.openr66.context.task.ExecBusinessTask";

	public BusinessRequest(NetworkTransaction networkTransaction,
			R66Future future, String remoteHost, BusinessRequestPacket packet) {
		super(BusinessRequest.class, future, remoteHost, networkTransaction, packet);
	}

	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(
				null));
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(BusinessRequest.class);
		}
		if (args.length < 5) {
			logger
					.error("Needs at least the configuration file as first argument and Business Java class and -to host as second and third arguments and -arg arguments\n"+
							_INFO_ARGS);
			return;
		}
		classname = DEFAULT_CLASS;
		if (!getParams(args) || classarg == null) {
			logger.error("Wrong initialization");
			if (DbConstant.admin != null && DbConstant.admin.isConnected) {
				DbConstant.admin.close();
			}
			ChannelUtils.stopLogger();
			System.exit(2);
		}
		Configuration.configuration.pipelineInit();
		NetworkTransaction networkTransaction = new NetworkTransaction();
		R66Future future = new R66Future(true);

		logger.info("Start Test of Transaction");
		long time1 = System.currentTimeMillis();

		BusinessRequestPacket packet =
				new BusinessRequestPacket(classname + " " + classarg, 0);
		BusinessRequest transaction = new BusinessRequest(
				networkTransaction, future, rhost, packet);
		transaction.run();
		future.awaitUninterruptibly();

		long time2 = System.currentTimeMillis();
		logger.debug("Finish Business Request: " + future.isSuccess());
		long delay = time2 - time1;
		if (future.isSuccess()) {
			logger.info("Business Request in status: SUCCESS" +
					"    <REMOTE>" + rhost + "</REMOTE>" +
					"    delay: " + delay);
		} else {
			logger.info("Business Request in status: FAILURE" +
					"    <REMOTE>" + rhost + "</REMOTE>" +
					"    <ERROR>" + future.getCause() + "</ERROR>" +
					"    delay: " + delay);
			networkTransaction.closeAll();
			System.exit(ErrorCode.Unknown.ordinal());
		}
		networkTransaction.closeAll();
	}

}
