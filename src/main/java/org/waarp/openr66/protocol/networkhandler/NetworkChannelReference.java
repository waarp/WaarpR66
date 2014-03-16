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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;

/**
 * NetworkChannelReference object to keep Network channel open while some local channels are attached to it.
 * 
 * @author Frederic Bregier
 * 
 */
public class NetworkChannelReference {
	/**
	 * Internal Logger
	 */
	protected static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(NetworkChannelReference.class);
	
	/**
	 * Does this Network Channel is in shutdown
	 */
	protected volatile boolean isShuttingDown = false;
	/**
	 * Associated LocalChannelReference
	 */
	private ConcurrentLinkedQueue<LocalChannelReference> localChannelReferences =
			new ConcurrentLinkedQueue<LocalChannelReference>();
	/**
	 * Network Channel
	 */
	protected final Channel channel;
	/**
	 * Remote network address (when valid)
	 */
	protected final SocketAddress networkAddress;
	/**
	 * Remote IP address
	 */
	private final String hostAddress;
	/**
	 * Remote Host Id
	 */
	private String hostId;
	/**
	 * ClientNetworkChannels object that contains this NetworkChannelReference
	 */
	protected ClientNetworkChannels clientNetworkChannels;
	/**
	 * Associated lock
	 */
	protected final ReentrantLock lock;
	/**
	 * Last Time in ms this channel was used by a LocalChannel
	 */
	private long lastTimeUsed = System.currentTimeMillis();

	public NetworkChannelReference(Channel networkChannel, ReentrantLock lock) {
		this.channel = networkChannel;
		this.networkAddress = channel.getRemoteAddress();
		this.hostAddress = ((InetSocketAddress) this.networkAddress).getAddress().getHostAddress();
		this.lock = lock;
	}
	
	public NetworkChannelReference(SocketAddress address, ReentrantLock lock) {
		this.channel = null;
		this.networkAddress = address;
		this.hostAddress = ((InetSocketAddress) this.networkAddress).getAddress().getHostAddress();
		this.lock = lock;
	}

	public void add(LocalChannelReference localChannel)
			throws OpenR66ProtocolRemoteShutdownException {
		// lock is of no use since caller is itself in locked situation for the very same lock
		if (isShuttingDown) {
			throw new OpenR66ProtocolRemoteShutdownException("Current NetworkChannelReference is closed");
		}
		use();
		localChannelReferences.add(localChannel);
	}
	
	/**
	 * To set the last time used
	 */
	public void use() {
		if (! isShuttingDown) {
			lastTimeUsed = System.currentTimeMillis();
		}
	}
	/**
	 * To set the last time used when correct
	 * @return True if last time used is set
	 */
	public boolean useIfUsed() {
		if (! isShuttingDown && ! localChannelReferences.isEmpty()) {
			lastTimeUsed = System.currentTimeMillis();
			return true;
		}
		return false;
	}
	
	/**
	 * Remove one LocalChanelReference, closing it if necessary.
	 * @param localChannel
	 */
	public void remove(LocalChannelReference localChannel) {
		lock.lock();
		try {
			if (localChannel.getLocalChannel().isConnected()) {
				Channels.close(localChannel.getLocalChannel());
			}
			localChannelReferences.remove(localChannel);
			//Do not since it prevents shutdown: lastTimeUsed = System.currentTimeMillis();
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Shutdown All Local Channels associated with this NCR
	 */
	public void shutdownAllLocalChannels() {
		isShuttingDown = true;
		LocalChannelReference [] localChannelReferenceArray = localChannelReferences.toArray(new LocalChannelReference[0]);
		ArrayList<LocalChannelReference> toCloseLater = new ArrayList<LocalChannelReference>();
		for (LocalChannelReference localChannelReference : localChannelReferenceArray) {
			if (!localChannelReference.getFutureRequest().isDone()) {
				if (localChannelReference.getFutureValidRequest().isDone() &&
						localChannelReference.getFutureValidRequest().isFailed()) {
					toCloseLater.add(localChannelReference);
					continue;
				} else {
					R66Result finalValue = new R66Result(
							localChannelReference.getSession(), true,
							ErrorCode.Shutdown, null);
					if (localChannelReference.getSession() != null) {
						try {
							localChannelReference.getSession().tryFinalizeRequest(finalValue);
						} catch (OpenR66RunnerErrorException e) {
						} catch (OpenR66ProtocolSystemException e) {
						}
					}
				}
			}
			Channels.close(localChannelReference.getLocalChannel());
		}
		try {
			Thread.sleep(Configuration.RETRYINMS*20);
		} catch (InterruptedException e) {
		}
		for (LocalChannelReference localChannelReference : toCloseLater) {
			Channels.close(localChannelReference.getLocalChannel());
		}
		toCloseLater.clear();
	}
	
	public int nbLocalChannels() {
		return localChannelReferences.size();
	}
	
	@Override
	public String toString() {
		return "NC: " + hostId+":"+ (channel != null ? channel.isConnected() : false) + " " +
				networkAddress + " Count: " + localChannelReferences.size();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof NetworkChannelReference) {
			NetworkChannelReference obj2 = (NetworkChannelReference) obj;
			if (obj2.channel == null || this.channel == null) {
				return false;
			}
			return (obj2.channel.getId().compareTo(this.channel.getId()) == 0);
		}
		return false;
	}

	@Override
	public int hashCode() {
		if (this.channel == null) {
			return Integer.MIN_VALUE;
		}
		return this.channel.getId();
	}

	/**
	 * 
	 * @return the hashcode for the global remote networkaddress
	 */
	public int getSocketHashCode() {
		return this.networkAddress.hashCode();
	}
	/**
	 * Used for BlackList
	 * @return the hashcode for the address
	 */
	public int getAddressHashCode() {
		return this.hostAddress.hashCode();
	}
	/**
	 * Check if the last time used is ok with a delay applied to the current time (timeout)
	 * @param delay
	 * @return <= 0 if OK, else > 0 (should send a KeepAlive or wait that time in ms)
	 */
	public long checkLastTime(long delay) {
		return (lastTimeUsed + delay - System.currentTimeMillis());
	}

	/**
	 * @return the isShuttingDown
	 */
	public boolean isShuttingDown() {
		return isShuttingDown;
	}

	/**
	 * @return the channel
	 */
	public Channel getChannel() {
		return channel;
	}

	/**
	 * @return the hostId
	 */
	public String getHostId() {
		return hostId;
	}
	
	/**
	 * @param hostId the hostId to set
	 */
	public void setHostId(String hostId) {
		this.hostId = hostId;
	}

	/**
	 * @return the lock
	 */
	public ReentrantLock getLock() {
		return lock;
	}

	/**
	 * @return the lastTimeUsed
	 */
	public long getLastTimeUsed() {
		return lastTimeUsed;
	}
	
}
