/**
   This file is part of GoldenGate Project (named also GoldenGate or GG).

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All GoldenGate Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   GoldenGate is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GoldenGate .  If not, see <http://www.gnu.org/licenses/>.
 */
package openr66.protocol.networkhandler.packet;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.protocol.configuration.Configuration;

import org.jboss.netty.util.ObjectSizeEstimator;

/**
 * Network Packet size estimator
 *
 * @author Frederic Bregier
 *
 */
public class NetworkPacketSizeEstimator implements ObjectSizeEstimator {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(NetworkPacketSizeEstimator.class);
    /*
     * (non-Javadoc)
     *
     * @see
     * org.jboss.netty.handler.execution.ObjectSizeEstimator#estimateSize(java
     * .lang.Object)
     */
    public int estimateSize(Object o) {
        if (!(o instanceof NetworkPacket)) {
            // Type unimplemented
            logger.error("Not NetworkPacket: "+o.getClass().getName());
            return Configuration.configuration.BLOCKSIZE+13;
        }
        NetworkPacket packet = (NetworkPacket) o;
        int size = packet.getBuffer().readableBytes()+13;
        return size;
    }
}
