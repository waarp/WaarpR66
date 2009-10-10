/**
 * Copyright 2009, Frederic Bregier, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package openr66.protocol.networkhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.util.concurrent.Executor;

import org.jboss.netty.handler.traffic.ChannelTrafficShapingHandler;
import org.jboss.netty.handler.traffic.TrafficCounter;
import org.jboss.netty.util.ObjectSizeEstimator;

/**
 * @author Frederic Bregier
 *
 */
public class ChannelTrafficHandler extends ChannelTrafficShapingHandler {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(ChannelTrafficHandler.class);
    /**
     * @param objectSizeEstimator
     * @param executor
     * @param writeLimit
     * @param readLimit
     * @param checkInterval
     */
    public ChannelTrafficHandler(ObjectSizeEstimator objectSizeEstimator,
            Executor executor, long writeLimit, long readLimit,
            long checkInterval) {
        super(objectSizeEstimator, executor, writeLimit, readLimit,
                checkInterval);
    }

    /* (non-Javadoc)
     * @see org.jboss.netty.handler.traffic.AbstractTrafficShapingHandler#doAccounting(org.jboss.netty.handler.traffic.TrafficCounter)
     */
    @Override
    protected void doAccounting(TrafficCounter counter) {
        logger.info(this.toString()+"\n   {}", counter);
    }

}
