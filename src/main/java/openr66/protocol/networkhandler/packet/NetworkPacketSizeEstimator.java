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
package openr66.protocol.networkhandler.packet;

import openr66.protocol.configuration.Configuration;

import org.jboss.netty.util.ObjectSizeEstimator;

/**
 * Network Packet size estimator
 *
 * @author Frederic Bregier
 *
 */
public class NetworkPacketSizeEstimator implements ObjectSizeEstimator {
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
            return Configuration.configuration.BLOCKSIZE+9;
        }
        NetworkPacket packet = (NetworkPacket) o;
        return packet.getBuffer().readableBytes()+9;
    }
}
