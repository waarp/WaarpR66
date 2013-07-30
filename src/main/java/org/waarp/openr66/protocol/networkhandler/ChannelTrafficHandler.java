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

import org.jboss.netty.handler.traffic.ChannelTrafficShapingHandler;
import org.jboss.netty.handler.traffic.TrafficCounter;
import org.jboss.netty.util.ObjectSizeEstimator;
import org.jboss.netty.util.Timer;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;

/**
 * @author Frederic Bregier
 * 
 */
public class ChannelTrafficHandler extends ChannelTrafficShapingHandler {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(ChannelTrafficHandler.class);

	/**
	 * @param objectSizeEstimator
	 * @param timer
	 * @param writeLimit
	 * @param readLimit
	 * @param checkInterval
	 */
	public ChannelTrafficHandler(ObjectSizeEstimator objectSizeEstimator,
			Timer timer, long writeLimit, long readLimit,
			long checkInterval) {
		super(objectSizeEstimator, timer, writeLimit, readLimit,
				checkInterval);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * org.jboss.netty.handler.traffic.AbstractTrafficShapingHandler#doAccounting(org.jboss.netty
	 * .handler.traffic.TrafficCounter)
	 */
	@SuppressWarnings("unused")
	@Override
	protected void doAccounting(TrafficCounter counter) {
		if (false)
			logger.debug(this.toString() + "    {}", counter);
	}

}
