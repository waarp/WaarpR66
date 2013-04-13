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
package org.waarp.openr66.protocol.localhandler.packet;

import org.jboss.netty.util.DefaultObjectSizeEstimator;
import org.jboss.netty.util.ObjectSizeEstimator;

/**
 * Local Packet size estimator
 * 
 * @author Frederic Bregier
 * 
 */
public class LocalPacketSizeEstimator implements ObjectSizeEstimator {
	private DefaultObjectSizeEstimator internal = new DefaultObjectSizeEstimator();

	/*
	 * (non-Javadoc)
	 * @see org.jboss.netty.handler.execution.ObjectSizeEstimator#estimateSize(java .lang.Object)
	 */
	public int estimateSize(Object o) {
		if (!(o instanceof AbstractLocalPacket)) {
			// Type unimplemented
			return internal.estimateSize(o);
		}
		AbstractLocalPacket packet = (AbstractLocalPacket) o;
		int size = packet.header.readableBytes() + packet.middle.readableBytes() +
				packet.end.readableBytes();
		return size;
	}
}
