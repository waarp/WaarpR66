/**
   This file is part of Waarp Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All Waarp Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   Waarp is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Waarp .  If not, see <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.service;

import org.jboss.netty.util.internal.SystemPropertyUtil;
import org.waarp.common.future.WaarpFuture;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.service.EngineAbstract;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.server.R66Server;

/**
 * Engine used to start and stop the real R66 service
 * @author Frederic Bregier
 *
 */
public class R66Engine extends EngineAbstract {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(R66Engine.class);
	
	protected WaarpFuture closeFurure = new WaarpFuture(true);
	
	public static final String CONFIGFILE = "org.waarp.r66.config.file";
	
	@Override
	public void run() {
		String []args = { "" };
		args[0] = SystemPropertyUtil.get(CONFIGFILE);
		if (args[0] == null) {
			logger.error("Cannot find "+CONFIGFILE+" parameter");
			shutdown();
			return;
		}
		try {
			R66Server.main(args);
		} catch (OpenR66ProtocolPacketException e) {
			logger.error("Cannot start R66", e);
			shutdown();
			return;
		}
		logger.warn("Service started with "+args[0]);
	}

	@Override
	public void shutdown() {
		Thread thread = new Thread(new ChannelUtils(), "R66 Shutdown Thread");
		thread.setDaemon(true);
		thread.start();
		closeFurure.setSuccess();
		logger.info("Service stopped");
	}

	@Override
	public boolean isShutdown() {
		return closeFurure.isDone();
	}

	@Override
	public boolean waitShutdown() throws InterruptedException {
		closeFurure.await();
		return closeFurure.isSuccess();
	}
}
