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
 * You should have received a copy of the GNU General Public License along with Waarp. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.protocol.localhandler;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.jboss.netty.channel.local.LocalAddress;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66FiniteDualStates;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolShutdownException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;
import org.waarp.openr66.protocol.localhandler.packet.LocalPacketFactory;
import org.waarp.openr66.protocol.localhandler.packet.StartupPacket;
import org.waarp.openr66.protocol.localhandler.packet.ValidPacket;
import org.waarp.openr66.protocol.networkhandler.NetworkChannelReference;
import org.waarp.openr66.protocol.networkhandler.packet.NetworkPacket;
import org.waarp.openr66.protocol.utils.R66Future;
import org.waarp.openr66.protocol.utils.R66ShutdownHook;

/**
 * This class handles Local Transaction connections
 * 
 * @author frederic bregier
 */
public class LocalTransaction {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(LocalTransaction.class);

	/**
	 * HashMap of LocalChannelReference using LocalChannelId
	 */
	private final ConcurrentHashMap<Integer, LocalChannelReference> localChannelHashMap = new ConcurrentHashMap<Integer, LocalChannelReference>();

	/**
	 * HashMap of LocalChannelReference using requested_requester_specialId
	 */
	private final ConcurrentHashMap<String, LocalChannelReference> localChannelHashMapIdBased = new ConcurrentHashMap<String, LocalChannelReference>();

	private final ChannelFactory channelServerFactory = new DefaultLocalServerChannelFactory();

	private final ServerBootstrap serverBootstrap = new ServerBootstrap(channelServerFactory);

	private final Channel serverChannel;

	private final LocalAddress socketLocalServerAddress = new LocalAddress("0");

	private final ChannelFactory channelClientFactory = new DefaultLocalClientChannelFactory();

	private final ClientBootstrap clientBootstrap = new ClientBootstrap(channelClientFactory);

	private final ChannelGroup localChannelGroup = new DefaultChannelGroup("LocalChannels");

	/**
	 * Constructor
	 */
	public LocalTransaction() {
		serverBootstrap.setPipelineFactory(new LocalServerPipelineFactory());
		serverBootstrap.setOption("connectTimeoutMillis",
				Configuration.configuration.TIMEOUTCON);
		serverChannel = serverBootstrap.bind(socketLocalServerAddress);
		localChannelGroup.add(serverChannel);
		clientBootstrap.setPipelineFactory(new LocalClientPipelineFactory());
	}

	public String hashStatus() {
		return "LocalTransaction: [localChannelHashMap: "+localChannelHashMap.size()+" localChannelHashMapIdBased: "+localChannelHashMapIdBased.size()+"] ";
	}
	/**
	 * Get the corresponding LocalChannelReference and set the remoteId if different
	 * 
	 * @param remoteId
	 * @param localId
	 * @return the LocalChannelReference
	 * @throws OpenR66ProtocolSystemException
	 */
	public LocalChannelReference getClient(Integer remoteId, Integer localId)
			throws OpenR66ProtocolSystemException {
		LocalChannelReference localChannelReference = getFromId(localId);
		if (localChannelReference != null) {
			if (localChannelReference.getRemoteId().compareTo(remoteId) != 0) {
				localChannelReference.setRemoteId(remoteId);
			}
			return localChannelReference;
		}
		throw new OpenR66ProtocolSystemException(
				"Cannot find LocalChannelReference");
	}

	/**
	 * Create a new Client
	 * 
	 * @param networkChannelReference
	 * @param remoteId might be set to ChannelUtils.NOCHANNEL (real creation)
	 * @param futureRequest might be null (from NetworkChannel Startup)
	 * @return the LocalChannelReference
	 * @throws OpenR66ProtocolSystemException
	 * @throws OpenR66ProtocolRemoteShutdownException 
	 * @throws OpenR66ProtocolNoConnectionException 
	 */
	public LocalChannelReference createNewClient(NetworkChannelReference networkChannelReference,
			Integer remoteId, R66Future futureRequest)
			throws OpenR66ProtocolSystemException, OpenR66ProtocolRemoteShutdownException, OpenR66ProtocolNoConnectionException {
		networkChannelReference.getLock().lock();
		try {
			ChannelFuture channelFuture = null;
			logger.debug("Status LocalChannelServer: {} {}", serverChannel
					.getClass().getName(), serverChannel.getConfig()
					.getConnectTimeoutMillis() + " " + serverChannel.isBound());
			for (int i = 0; i < Configuration.RETRYNB; i++) {
				if (R66ShutdownHook.isShutdownStarting()) {
					// Do not try since already locally in shutdown
					throw new OpenR66ProtocolNoConnectionException(
							"Cannot connect to local handler: " + socketLocalServerAddress +
									" " + serverChannel.isBound() + " " + serverChannel +
									" since the local server is in shutdown.");
				}
				channelFuture = clientBootstrap.connect(socketLocalServerAddress);
				try {
					channelFuture.await();
				} catch (InterruptedException e1) {
					logger.error("LocalChannelServer Interrupted: " +
							serverChannel.getClass().getName() + " " +
							serverChannel.getConfig().getConnectTimeoutMillis() +
							" " + serverChannel.isBound());
					throw new OpenR66ProtocolSystemException(
							"Interruption - Cannot connect to local handler: " +
									socketLocalServerAddress + " " +
									serverChannel.isBound() + " " + serverChannel,
							e1);
				}
				if (channelFuture.isSuccess()) {
					final Channel channel = channelFuture.getChannel();
					localChannelGroup.add(channel);
					logger.debug("Will start localChannelReference and eventually generate a new Db Connection if not-thread-safe");
					final LocalChannelReference localChannelReference = new LocalChannelReference(
							channel, networkChannelReference, remoteId, futureRequest);
					localChannelHashMap.put(channel.getId(), localChannelReference);
					logger.debug("Db connection done and Create LocalChannel entry: " + i + " {}",
							localChannelReference);
					logger.info("Add one localChannel to a Network Channel: "+channel.getId());
					// Now send first a Startup message
					StartupPacket startup = new StartupPacket(
							localChannelReference.getLocalId());
					try {
						Channels.write(channel, startup).await();
					} catch (InterruptedException e) {
						logger.error("Can't connect to local server due to interruption" + i);
						throw new OpenR66ProtocolSystemException(
								"Cannot connect to local handler", e);
					}
					return localChannelReference;
				} else {
					logger.error("Can't connect to local server " + i);
				}
				try {
					Thread.sleep(Configuration.RETRYINMS);
				} catch (InterruptedException e) {
					throw new OpenR66ProtocolSystemException(
							"Cannot connect to local handler", e);
				}
			}
			logger.error("LocalChannelServer: " +
					serverChannel.getClass().getName() + " " +
					serverChannel.getConfig().getConnectTimeoutMillis() + " " +
					serverChannel.isBound());
			throw new OpenR66ProtocolSystemException(
					"Cannot connect to local handler: " + socketLocalServerAddress +
							" " + serverChannel.isBound() + " " + serverChannel,
					channelFuture.getCause());
		} finally {
			networkChannelReference.getLock().unlock();
		}
	}

	/**
	 * 
	 * @param id
	 * @return the LocalChannelReference
	 */
	public LocalChannelReference getFromId(Integer id) {
		int maxtry = (int) (Configuration.configuration.TIMEOUTCON / Configuration.RETRYINMS) / 2;
		for (int i = 0; i < maxtry; i++) {
			LocalChannelReference lcr = localChannelHashMap.get(id);
			if (lcr == null) {
				try {
					Thread.sleep(Configuration.RETRYINMS);
					Thread.yield();
				} catch (InterruptedException e) {
				}
			} else {
				return lcr;
			}
		}
		return localChannelHashMap.get(id);
	}
	
	/**
	 * Remove one local channel
	 * 
	 * @param localChannelReference
	 */
	protected void remove(LocalChannelReference localChannelReference) {
		logger.debug("DEBUG remove: "+localChannelReference.getLocalId());
		localChannelHashMap.remove(localChannelReference.getLocalId());
		if (localChannelReference.getRequestId() != null) {
			localChannelHashMapIdBased.remove(localChannelReference.getRequestId());
		}
	}

	/**
	 * 
	 * @param runner
	 * @param lcr
	 */
	public void setFromId(DbTaskRunner runner, LocalChannelReference lcr) {
		String key = runner.getKey();
		lcr.setRequestId(key);
		localChannelHashMapIdBased.put(key, lcr);
	}

	/**
	 * 
	 * @param key
	 *            as "requested requester specialId"
	 * @return the LocalChannelReference
	 */
	public LocalChannelReference getFromRequest(String key) {
		return localChannelHashMapIdBased.get(key);
	}

	/**
	 * 
	 * @return the number of active local channels
	 */
	public int getNumberLocalChannel() {
		return localChannelHashMap.size();
	}

	/**
	 * Debug function (while shutdown for instance)
	 */
	public void debugPrintActiveLocalChannels() {
		Collection<LocalChannelReference> collection = localChannelHashMap.values();
		Iterator<LocalChannelReference> iterator = collection.iterator();
		while (iterator.hasNext()) {
			LocalChannelReference localChannelReference = iterator.next();
			logger.debug("Will close local channel: {}", localChannelReference);
			logger.debug(
					" Containing: {}",
					(localChannelReference.getSession() != null ? localChannelReference
							.getSession() : "no session"));
		}
	}

	/**
	 * Informs all remote client that the server is shutting down
	 */
	public void shutdownLocalChannels() {
		logger.warn("Will inform LocalChannels of Shutdown: " + localChannelHashMap.size());
		Collection<LocalChannelReference> collection = localChannelHashMap.values();
		Iterator<LocalChannelReference> iterator = collection.iterator();
		ValidPacket packet = new ValidPacket("Shutdown forced", null,
				LocalPacketFactory.SHUTDOWNPACKET);
		ChannelBuffer buffer = null;
		while (iterator.hasNext()) {
			LocalChannelReference localChannelReference = iterator.next();
			logger.info("Inform Shutdown {}", localChannelReference);
			packet.setSmiddle(null);
			// If a transfer is running, save the current rank and inform remote
			// host
			if (localChannelReference.getSession() != null) {
				R66Session session = localChannelReference.getSession();
				DbTaskRunner runner = session.getRunner();
				if (runner != null && runner.isInTransfer()) {
					if (!runner.isSender()) {
						int newrank = runner.getRank();
						packet.setSmiddle(Integer.toString(newrank));
					}
					// Save File status
					try {
						runner.saveStatus();
					} catch (OpenR66RunnerErrorException e) {
					}
				}
				if (runner != null && ! runner.isFinished()) {
					R66Result result = new R66Result(
							new OpenR66ProtocolShutdownException(), session,
							true, ErrorCode.Shutdown, runner);
					result.other = packet;
					try {
						buffer = packet.getLocalPacket(localChannelReference);
					} catch (OpenR66ProtocolPacketException e1) {
					}
					localChannelReference.sessionNewState(R66FiniteDualStates.SHUTDOWN);
					NetworkPacket message = new NetworkPacket(
							localChannelReference.getLocalId(),
							localChannelReference.getRemoteId(),
							packet.getType(), buffer);
					try {
						Channels.write(localChannelReference.getNetworkChannel(),
								message).await();
					} catch (InterruptedException e1) {
					}
					try {
						session.setFinalizeTransfer(false, result);
					} catch (OpenR66RunnerErrorException e) {
					} catch (OpenR66ProtocolSystemException e) {
					}
				}
				Channels.close(localChannelReference.getLocalChannel());
				continue;
			}
			try {
				buffer = packet.getLocalPacket(localChannelReference);
			} catch (OpenR66ProtocolPacketException e1) {
			}
			NetworkPacket message = new NetworkPacket(
					localChannelReference.getLocalId(),
					localChannelReference.getRemoteId(), packet.getType(),
					buffer);
			Channels.write(localChannelReference.getNetworkChannel(), message);
		}
	}

	/**
	 * Close All Local Channels
	 */
	public void closeAll() {
		logger.debug("close All Local Channels");
		localChannelGroup.close().awaitUninterruptibly();
		clientBootstrap.releaseExternalResources();
		channelClientFactory.releaseExternalResources();
		serverBootstrap.releaseExternalResources();
		channelServerFactory.releaseExternalResources();
	}

}
