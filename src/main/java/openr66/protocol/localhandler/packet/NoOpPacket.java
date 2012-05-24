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
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * No Op class
 *
 * header = empty middle = empty end = empty
 *
 * @author frederic bregier
 */
public class NoOpPacket extends AbstractLocalPacket {

    /**
     * @param headerLength
     * @param middleLength
     * @param endLength
     * @param buf
     * @return the new EndTransferPacket from buffer
     * @throws OpenR66ProtocolPacketException
     */
    public static NoOpPacket createFromBuffer(int headerLength,
            int middleLength, int endLength, ChannelBuffer buf)
            throws OpenR66ProtocolPacketException {
        return new NoOpPacket();
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#createEnd()
     */
    @Override
    public void createEnd() {
        end = ChannelBuffers.EMPTY_BUFFER;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * openr66.protocol.localhandler.packet.AbstractLocalPacket#createHeader()
     */
    @Override
    public void createHeader() {
        header = ChannelBuffers.EMPTY_BUFFER;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * openr66.protocol.localhandler.packet.AbstractLocalPacket#createMiddle()
     */
    @Override
    public void createMiddle() {
        middle = ChannelBuffers.EMPTY_BUFFER;
    }

    @Override
    public byte getType() {
        return LocalPacketFactory.NOOPPACKET;
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.protocol.localhandler.packet.AbstractLocalPacket#toString()
     */
    @Override
    public String toString() {
        return "NoOpPacket";
    }

}
