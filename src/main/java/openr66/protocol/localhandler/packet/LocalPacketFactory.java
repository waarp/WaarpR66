/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors. This is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of the License,
 * or (at your option) any later version. This software is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * software; if not, write to the Free Software Foundation, Inc., 51 Franklin
 * St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF site:
 * http://www.fsf.org.
 */
package openr66.protocol.localhandler.packet;

import openr66.protocol.exception.OpenR66ProtocolPacketException;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Factory to create Packet according to type from a buffer
 *
 * @author Frederic Bregier
 */
public class LocalPacketFactory {
    public static final byte AUTHENTPACKET = 1;

    public static final byte STARTUPPACKET = 2;

    public static final byte DATAPACKET = 3;

    public static final byte VALIDPACKET = 4;

    public static final byte ERRORPACKET = 5;

    public static final byte CONNECTERRORPACKET = 6;

    public static final byte REQUESTPACKET = 7;

    public static final byte SHUTDOWNPACKET = 8;

    public static final byte STOPPACKET = 9;

    public static final byte CANCELPACKET = 10;

    public static final byte CONFIGSENDPACKET = 11;

    public static final byte CONFIGRECVPACKET = 12;

    public static final byte TESTPACKET = 13;

    public static final byte ENDTRANSFERPACKET = 14;

    public static final byte REQUESTUSERPACKET = 15;

    public static final byte LOGPACKET = 16;

    public static final byte LOGPURGEPACKET = 17;

    public static final byte INFORMATIONPACKET = 18;

    public static final byte BANDWIDTHPACKET = 19;


    /**
     * This method create a Packet from the ChannelBuffer.
     *
     * @param headerLength
     *            length of the header from the current position of the buffer
     * @param middleLength
     * @param endLength
     * @param buf
     * @return the newly created Packet
     * @throws OpenR66ProtocolPacketException
     */
    public static AbstractLocalPacket createPacketFromChannelBuffer(
            int headerLength, int middleLength, int endLength, ChannelBuffer buf)
            throws OpenR66ProtocolPacketException {
        final byte packetType = buf.readByte();
        switch (packetType) {
            case AUTHENTPACKET:
                return AuthentPacket.createFromBuffer(headerLength,
                        middleLength, endLength, buf);
            case STARTUPPACKET:
                return StartupPacket.createFromBuffer(headerLength,
                        middleLength, endLength, buf);
            case DATAPACKET:
                return DataPacket.createFromBuffer(headerLength, middleLength,
                        endLength, buf);
            case VALIDPACKET:
                return ValidPacket.createFromBuffer(headerLength, middleLength,
                        endLength, buf);
            case ERRORPACKET:
                return ErrorPacket.createFromBuffer(headerLength, middleLength,
                        endLength, buf);
            case CONNECTERRORPACKET:
                return ConnectionErrorPacket.createFromBuffer(headerLength,
                        middleLength, endLength, buf);
            case REQUESTPACKET:
                return RequestPacket.createFromBuffer(headerLength,
                        middleLength, endLength, buf);
            case SHUTDOWNPACKET:
                return ShutdownPacket.createFromBuffer(headerLength,
                        middleLength, endLength, buf);
            case STOPPACKET:
            case CANCELPACKET:
            case REQUESTUSERPACKET:
            case CONFIGSENDPACKET:
            case CONFIGRECVPACKET:
            case LOGPACKET:
            case LOGPURGEPACKET:
            case BANDWIDTHPACKET:
                throw new OpenR66ProtocolPacketException(
                        "Unimplemented Packet Type received: " + packetType);
            case TESTPACKET:
                return TestPacket.createFromBuffer(headerLength, middleLength,
                        endLength, buf);
            case ENDTRANSFERPACKET:
                return EndTransferPacket.createFromBuffer(headerLength,
                        middleLength, endLength, buf);
            case INFORMATIONPACKET:
                return InformationPacket.createFromBuffer(headerLength,
                        middleLength, endLength, buf);
            default:
                throw new OpenR66ProtocolPacketException(
                        "Unvalid Packet Type received: " + packetType);
        }
    }
}
