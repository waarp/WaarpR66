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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Client NetworkChannel attached to one HostId.
 * 
 * 
 * 
 * This class is used to keep information for one HostID when it connects as client to the current
 * Host. As one Id can be shared or one can use direct send, so having a connection by request, this
 * class is useful when one wants to know who is connected and how many times.
 * 
 * 
 * @author Frederic Bregier
 * 
 */
public class ClientNetworkChannels {

	private final String hostId;
	private final Set<NetworkChannel> networkChannels = Collections.newSetFromMap(new ConcurrentHashMap<NetworkChannel, Boolean>());

	public ClientNetworkChannels(String hostId) {
		this.hostId = hostId;
	}

	public void add(NetworkChannel networkChannel) {
		networkChannels.add(networkChannel);
	}

	public void remove(NetworkChannel networkChannel) {
		networkChannels.remove(networkChannel);
	}

	public boolean isEmpty() {
		return networkChannels.isEmpty();
	}

	public int size() {
		return networkChannels.size();
	}

	public boolean shutdownAll() {
		boolean status = false;
		for (NetworkChannel networkChannel : networkChannels) {
			NetworkTransaction.shuttingdownNetworkChannel(networkChannel.channel);
			status = true;
		}
		networkChannels.clear();
		return status;
	}

	public String getHostId() {
		return hostId;
	}
}
