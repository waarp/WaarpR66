/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License, or (at your option)
 * any later version.
 *
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package openr66.protocol.networkhandler;

import java.util.concurrent.ConcurrentLinkedQueue;

import openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;

/**
 * NetworkChannel object to keep channel open with local channels
 *
 * @author Frederic Bregier
 *
 */
public class NetworkChannel {
    /**
     * Number of active Local Channel referencing this Network Channel
     */
    public volatile int count = 1;
    /**
     * Does this Network Channel is in shutdown
     */
    public volatile boolean isShuttingDown = false;
    /**
     * Associated LocalChannel
     */
    public ConcurrentLinkedQueue<Channel> localChannels =
        new ConcurrentLinkedQueue<Channel>();
    /**
     * Network Channel
     */
    public final Channel channel;

    public NetworkChannel(Channel networkChannel) {
        this.channel = networkChannel;
    }

    synchronized public void add(Channel localChannel)
    throws OpenR66ProtocolRemoteShutdownException {
        if (isShuttingDown) {
            throw new OpenR66ProtocolRemoteShutdownException("Current NetworkChannel is closed");
        }
        localChannels.add(localChannel);
    }

    synchronized public void remove(Channel localChannel) {
        if (localChannel.isConnected()) {
            Channels.close(localChannel);
        }
        localChannels.remove(localChannel);
    }

    synchronized public void shutdownAllLocalChannels() {
        isShuttingDown = true;
        Channel localChannel = localChannels.poll();
        while (localChannel != null) {
            Channels.close(localChannel);
            localChannel = localChannels.poll();
        }
    }
    @Override
    public String toString() {
        return "NC: " + channel.isConnected() + " " +
                channel.getRemoteAddress() + " Count: " + count;
    }
}
