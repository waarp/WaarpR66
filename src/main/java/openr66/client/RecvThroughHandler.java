/**
 * This file is part of GoldenGate Project (named also GoldenGate or GG).
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All GoldenGate Project is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 * 
 * GoldenGate is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with GoldenGate . If not,
 * see <http://www.gnu.org/licenses/>.
 */
package openr66.client;

import openr66.protocol.exception.OpenR66ProtocolBusinessException;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Class to be implemented for {@link RecvThroughClient}
 * 
 * @author Frederic Bregier
 * 
 */
public abstract class RecvThroughHandler {
	/**
	 * This method will be called for each valid packet received to be written
	 * 
	 * @param buffer
	 * @exception OpenR66ProtocolBusinessException
	 *                This exception has to be throw if any error occurs during write in business
	 *                process.
	 */
	abstract public void writeChannelBuffer(ChannelBuffer buffer)
			throws OpenR66ProtocolBusinessException;

	/**
	 * Facility function to read from buffer and transfer to an array of bytes
	 * 
	 * @param buffer
	 * @return the array of bytes
	 */
	protected byte[] getByte(ChannelBuffer buffer) {
		byte[] dst = new byte[buffer.readableBytes()];
		buffer.readBytes(dst);
		return dst;
	}
}
