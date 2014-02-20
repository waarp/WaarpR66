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
package org.waarp.openr66.protocol.networkhandler;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;

/**
 * NetworkChannel object to keep Network channel open while some local channels are attached to it.
 * 
 * @author Frederic Bregier
 * 
 */
public class NetworkChannel {
	/**
	 * Number of active Local Channel referencing this Network Channel
	 */
	public AtomicInteger count = new AtomicInteger(1);
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
	/**
	 * Remote Host Id
	 */
	public String hostId;
	/**
	 * Last Time in ms this channel was used by a LocalChannel
	 */
	public long lastTimeUsed = System.currentTimeMillis();

	public NetworkChannel(Channel networkChannel) {
		this.channel = networkChannel;
	}

	synchronized public void add(Channel localChannel)
			throws OpenR66ProtocolRemoteShutdownException {
		if (isShuttingDown) {
			throw new OpenR66ProtocolRemoteShutdownException("Current NetworkChannel is closed");
		}
		lastTimeUsed = System.currentTimeMillis();
		localChannels.add(localChannel);
	}
	
	/**
	 * To set the last time used
	 */
	public void use() {
		lastTimeUsed = System.currentTimeMillis();
	}

	synchronized public void remove(Channel localChannel) {
		if (localChannel.isConnected()) {
			Channels.close(localChannel);
		}
		localChannels.remove(localChannel);
	}

	synchronized public void shutdownAllLocalChannels() {
		isShuttingDown = true;
		count.set(0);
		Channel localChannel = localChannels.poll();
		while (localChannel != null) {
			Channels.close(localChannel);
			localChannel = localChannels.poll();
		}
	}
	
	@Override
	public String toString() {
		return "NC: " + hostId+":"+ channel.isConnected() + " " +
				channel.getRemoteAddress() + " Count: " + count;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof NetworkChannel) {
			NetworkChannel obj2 = (NetworkChannel) obj;
			return (obj2.channel.getId().compareTo(this.channel.getId()) == 0);
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return this.channel.getId();
	}

}
