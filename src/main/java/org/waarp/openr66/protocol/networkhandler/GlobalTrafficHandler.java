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
package org.waarp.openr66.protocol.networkhandler;

import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.handler.traffic.TrafficCounter;
import io.netty.util.ObjectSizeEstimator;
import io.netty.util.Timer;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;

/**
 * Global function
 * 
 * @author Frederic Bregier
 * 
 */
public class GlobalTrafficHandler extends GlobalTrafficShapingHandler {
	/**
	 * Internal Logger
	 */
	private static final WaarpLogger logger = WaarpLoggerFactory
			.getLogger(GlobalTrafficHandler.class);

	/**
	 * @param objectSizeEstimator
	 * @param timer
	 * @param writeLimit
	 * @param readLimit
	 * @param checkInterval
	 */
	public GlobalTrafficHandler(ObjectSizeEstimator objectSizeEstimator,
			Timer timer, long writeLimit, long readLimit,
			long checkInterval) {
		super(objectSizeEstimator, timer, writeLimit, readLimit,
				checkInterval);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * io.netty.handler.traffic.AbstractTrafficShapingHandler#doAccounting(io.netty
	 * .handler.traffic.TrafficCounter)
	 */
	@SuppressWarnings("unused")
	@Override
	protected void doAccounting(TrafficCounter counter) {
		if (false)
			logger.debug(this.toString() + "    {}", counter);
	}

}
