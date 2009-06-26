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
package openr66.protocol.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Factory to create Packet according to type from a buffer
 * @author Frederic Bregier
 *
 */
public class PacketFactory {
	public static final byte TESTPACKET = 0;
    /**
     * This method create a Packet from the ChannelBuffer.
     * @param headerLength length of the header from the current position of the buffer
     * @param middleLength 
     * @param endLength 
     * @param buf
     * @return the newly created Packet
     * @throws OpenR66ProtocolPacketException 
     */
    public static AbstractPacket createPacketFromChannelBuffer(int headerLength, 
            int middleLength, int endLength, ChannelBuffer buf) throws OpenR66ProtocolPacketException {
    	byte packetType = buf.readByte();
    	switch (packetType) {
    		case TESTPACKET:
    			byte[] bheader = new byte[headerLength-1];
    			byte[] bmiddle = new byte[middleLength];
    			byte[] bend = new byte[endLength];
    			buf.readBytes(bheader);
    			buf.readBytes(bmiddle);
    			buf.readBytes(bend);
    			return new TestPacket(new String(bheader),
    					new String(bmiddle),
    					new String(bend));
    		default:
    			throw new OpenR66ProtocolPacketException("Unvalid Packet Type received: "+packetType);
    	}
    }
}
