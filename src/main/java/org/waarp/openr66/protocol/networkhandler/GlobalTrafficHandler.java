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
package org.waarp.openr66.protocol.networkhandler;

import org.jboss.netty.handler.traffic.GlobalChannelTrafficShapingHandler;
import org.jboss.netty.util.ObjectSizeEstimator;
import org.jboss.netty.util.Timer;
import org.waarp.openr66.protocol.networkhandler.packet.NetworkPacketSizeEstimator;

/**
 * Default Global Traffic (Global + Channel)
 * @author "Frederic Bregier"
 *
 */
public class GlobalTrafficHandler extends GlobalChannelTrafficShapingHandler {
    NetworkPacketSizeEstimator objectSizeEstimator = new NetworkPacketSizeEstimator();
    /**
     * @param timer
     * @param writeGlobalLimit
     * @param readGlobalLimit
     * @param writeChannelLimit
     * @param readChannelLimit
     * @param checkInterval
     * @param maxTime
     */
    public GlobalTrafficHandler(Timer timer, long writeGlobalLimit, long readGlobalLimit, long writeChannelLimit,
            long readChannelLimit, long checkInterval, long maxTime) {
        super(timer, writeGlobalLimit, readGlobalLimit, writeChannelLimit, readChannelLimit, checkInterval, maxTime);
    }

    /**
     * @param timer
     * @param writeGlobalLimit
     * @param readGlobalLimit
     * @param writeChannelLimit
     * @param readChannelLimit
     * @param checkInterval
     */
    public GlobalTrafficHandler(Timer timer, long writeGlobalLimit, long readGlobalLimit, long writeChannelLimit,
            long readChannelLimit, long checkInterval) {
        super(timer, writeGlobalLimit, readGlobalLimit, writeChannelLimit, readChannelLimit, checkInterval);
    }

    /**
     * @param objectSizeEstimator
     * @param timer
     * @param writeLimit
     * @param readLimit
     * @param writeChannelLimit
     * @param readChannelLimit
     * @param checkInterval
     * @param maxTime
     */
    public GlobalTrafficHandler(ObjectSizeEstimator objectSizeEstimator, Timer timer, long writeLimit, long readLimit,
            long writeChannelLimit, long readChannelLimit, long checkInterval, long maxTime) {
        super(objectSizeEstimator, timer, writeLimit, readLimit, writeChannelLimit, readChannelLimit, checkInterval, maxTime);
    }

    /**
     * @param objectSizeEstimator
     * @param timer
     * @param writeLimit
     * @param readLimit
     * @param writeChannelLimit
     * @param readChannelLimit
     * @param checkInterval
     */
    public GlobalTrafficHandler(ObjectSizeEstimator objectSizeEstimator, Timer timer, long writeLimit, long readLimit,
            long writeChannelLimit, long readChannelLimit, long checkInterval) {
        super(objectSizeEstimator, timer, writeLimit, readLimit, writeChannelLimit, readChannelLimit, checkInterval);
    }

}
