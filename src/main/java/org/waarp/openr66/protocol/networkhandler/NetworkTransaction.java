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

import static org.waarp.openr66.context.R66FiniteDualStates.AUTHENTR;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipelineException;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.waarp.common.crypto.ssl.WaarpSslUtility;
import org.waarp.common.database.DbAdmin;
import org.waarp.common.digest.FilesystemBasedDigest;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.utility.WaarpThreadFactory;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66Exception;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNetworkException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoDataException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoSslException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;
import org.waarp.openr66.protocol.localhandler.RetrieveRunner;
import org.waarp.openr66.protocol.localhandler.packet.AuthentPacket;
import org.waarp.openr66.protocol.localhandler.packet.ConnectionErrorPacket;
import org.waarp.openr66.protocol.networkhandler.packet.NetworkPacket;
import org.waarp.openr66.protocol.networkhandler.ssl.NetworkSslServerHandler;
import org.waarp.openr66.protocol.networkhandler.ssl.NetworkSslServerPipelineFactory;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;
import org.waarp.openr66.protocol.utils.R66ShutdownHook;

/**
 * This class handles Network Transaction connections
 * 
 * @author frederic bregier
 */
public class NetworkTransaction {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(NetworkTransaction.class);

	/**
	 * Hashmap for Currently Shutdown remote host
	 */
	private static final ConcurrentHashMap<Integer, NetworkChannel> networkChannelShutdownOnSocketAddressConcurrentHashMap = new ConcurrentHashMap<Integer, NetworkChannel>();
	/**
	 * Hashmap for Currently blacklisted remote host
	 */
	private static final ConcurrentHashMap<Integer, NetworkChannel> networkChannelBlacklistedOnInetSocketAddressConcurrentHashMap = new ConcurrentHashMap<Integer, NetworkChannel>();

	/**
	 * Hashmap for currently active remote host
	 */
	private static final ConcurrentHashMap<Integer, NetworkChannel> networkChannelOnSocketAddressConcurrentHashMap = new ConcurrentHashMap<Integer, NetworkChannel>();
	/**
	 * Hashmap for lock based on remote address
	 */
	private static final ConcurrentHashMap<Integer, ReentrantLock> reentrantLockOnSocketAddressConcurrentHashMap = new ConcurrentHashMap<Integer, ReentrantLock>();
	/**
	 * Remote Client NetworkChannels
	 */
	private static final ConcurrentHashMap<String, ClientNetworkChannels> remoteClients = new ConcurrentHashMap<String, ClientNetworkChannels>();
	/**
	 * Remote Client NetworkChannels per NetworkChannel
	 */
	private static final ConcurrentHashMap<Integer, ClientNetworkChannels> remoteClientsPerNetworkChannel = new ConcurrentHashMap<Integer, ClientNetworkChannels>();
	/**
	 * Lock for Client NetworkChannels operations
	 */
	private static final ReentrantLock lockClient = new ReentrantLock();
	/**
	 * Hashmap for currently active Retrieve Runner (sender)
	 */
	private static final ConcurrentHashMap<Integer, RetrieveRunner> retrieveRunnerConcurrentHashMap =
			new ConcurrentHashMap<Integer, RetrieveRunner>();

	/**
	 * Lock for NetworkChannel operations
	 */
	private static final ReentrantLock lock = new ReentrantLock();

	/**
	 * ExecutorService for RetrieveOperation
	 */
	private static final ExecutorService retrieveExecutor = Executors
			.newCachedThreadPool(new WaarpThreadFactory("RetrieveExecutor"));

	/**
	 * ExecutorService Server Boss
	 */
	private final ExecutorService execServerBoss = Executors
			.newCachedThreadPool(new WaarpThreadFactory("ServerBossRetrieve"));

	/**
	 * ExecutorService Server Worker
	 */
	private final ExecutorService execServerWorker = Executors
			.newCachedThreadPool(new WaarpThreadFactory("ServerWorkerRetrieve"));

	private final ChannelFactory channelClientFactory = new NioClientSocketChannelFactory(
			execServerBoss,
			execServerWorker,
			Configuration.configuration.CLIENT_THREAD);

	private final ClientBootstrap clientBootstrap = new ClientBootstrap(
			channelClientFactory);
	private final ClientBootstrap clientSslBootstrap = new ClientBootstrap(
			channelClientFactory);
	private final ChannelGroup networkChannelGroup = new DefaultChannelGroup(
			"NetworkChannels");

	public NetworkTransaction() {
		NetworkServerPipelineFactory networkServerPipelineFactory = new NetworkServerPipelineFactory(false);
		clientBootstrap.setPipelineFactory(networkServerPipelineFactory);
		clientBootstrap.setOption("tcpNoDelay", true);
		clientBootstrap.setOption("reuseAddress", true);
		clientBootstrap.setOption("connectTimeoutMillis",
				Configuration.configuration.TIMEOUTCON);
		if (Configuration.configuration.useSSL && Configuration.configuration.HOST_SSLID != null) {
			NetworkSslServerPipelineFactory networkSslServerPipelineFactory =
					new NetworkSslServerPipelineFactory(true, execServerWorker);
			clientSslBootstrap.setPipelineFactory(networkSslServerPipelineFactory);
			clientSslBootstrap.setOption("tcpNoDelay", true);
			clientSslBootstrap.setOption("reuseAddress", true);
			clientSslBootstrap.setOption("connectTimeoutMillis",
					Configuration.configuration.TIMEOUTCON);
		} else {
			if (Configuration.configuration.warnOnStartup) {
				logger.warn("No SSL support configured");
			} else {
				logger.info("No SSL support configured");
			}
		}
	}

	private static ReentrantLock getChannelLock(SocketAddress socketAddress) {
		lock.lock();
		try {
			if (socketAddress == null) {
				// should not
				logger.info("SocketAddress empty here !");
				return lock;
			}
			Integer hash = socketAddress.hashCode();
			ReentrantLock socketLock = new ReentrantLock(true);
			reentrantLockOnSocketAddressConcurrentHashMap.putIfAbsent(hash, socketLock);
			socketLock = reentrantLockOnSocketAddressConcurrentHashMap.get(hash);
			return socketLock;
		} finally {
			lock.unlock();
		}
	}

	private static void removeChannelLock(SocketAddress socketAddress) {
		lock.lock();
		try {
			if (socketAddress == null) {
				// should not
				logger.info("SocketAddress empty here !");
				return;
			}
			Integer hash = socketAddress.hashCode();
			reentrantLockOnSocketAddressConcurrentHashMap.remove(hash);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Create a connection to the specified socketAddress with multiple retries
	 * 
	 * @param socketAddress
	 * @param isSSL
	 * @param futureRequest
	 * @return the LocalChannelReference
	 */
	public LocalChannelReference createConnectionWithRetry(SocketAddress socketAddress,
			boolean isSSL, R66Future futureRequest) {
		LocalChannelReference localChannelReference = null;
		OpenR66Exception lastException = null;
		for (int i = 0; i < Configuration.RETRYNB; i++) {
			try {
				localChannelReference =
						createConnection(socketAddress, isSSL, futureRequest);
				break;
			} catch (OpenR66ProtocolRemoteShutdownException e1) {
				lastException = e1;
				localChannelReference = null;
				break;
			} catch (OpenR66ProtocolNoConnectionException e1) {
				lastException = e1;
				localChannelReference = null;
				break;
			} catch (OpenR66ProtocolNetworkException e1) {
				// Can retry
				lastException = e1;
				localChannelReference = null;
				try {
					Thread.sleep(Configuration.WAITFORNETOP);
				} catch (InterruptedException e) {
					break;
				}
			}
		}
		if (localChannelReference == null) {
			logger.debug("Cannot connect : {}", lastException.getMessage());
		} else if (lastException != null) {
			logger.debug("Connection retried since {}", lastException.getMessage());
		}
		return localChannelReference;
	}

	/**
	 * Create a connection to the specified socketAddress
	 * 
	 * @param socketAddress
	 * @param isSSL
	 * @param futureRequest
	 * @return the LocalChannelReference
	 * @throws OpenR66ProtocolNetworkException
	 * @throws OpenR66ProtocolRemoteShutdownException
	 * @throws OpenR66ProtocolNoConnectionException
	 */
	public LocalChannelReference createConnection(SocketAddress socketAddress, boolean isSSL,
			R66Future futureRequest)
			throws OpenR66ProtocolNetworkException,
			OpenR66ProtocolRemoteShutdownException,
			OpenR66ProtocolNoConnectionException {
		NetworkChannel networkChannel = null;
		LocalChannelReference localChannelReference = null;
		boolean ok = false;
		// check valid limit on server side only (could be the initiator but not a client)
		DbHostAuth auth = isSSL ? Configuration.configuration.HOST_SSLAUTH : Configuration.configuration.HOST_AUTH;
		if (!auth.isClient()) {
			boolean valid = false;
			for (int i = 0; i < Configuration.RETRYNB * 2; i++) {
				if (Configuration.configuration.constraintLimitHandler.checkConstraintsSleep(i)) {
					logger.debug("Constraints exceeded: " + i);
				} else {
					logger.debug("Constraints NOT exceeded");
					valid = true;
					break;
				}
			}
			if (!valid) {
				// Limit is locally exceeded
				logger.debug("Overloaded local system");
				throw new OpenR66ProtocolNetworkException(
						"Cannot connect to remote server due to local overload");
			}
		}
		try {
			networkChannel = createNewConnection(socketAddress, isSSL);
			localChannelReference = createNewClient(networkChannel, futureRequest);
			ok = true;
		} finally {
			if (!ok) {
				if (networkChannel != null) {
					removeNetworkChannel(networkChannel.channel, null, null);
				}
			}
		}
		if (localChannelReference.getFutureValidateStartup().isDone() &&
				localChannelReference.getFutureValidateStartup().isSuccess()) {
			sendValidationConnection(localChannelReference);
		} else {
			OpenR66ProtocolNetworkException exc =
					new OpenR66ProtocolNetworkException("Startup is invalid");
			logger.debug("Startup is Invalid", exc);
			R66Result finalValue = new R66Result(
					exc, null, true, ErrorCode.ConnectionImpossible, null);
			localChannelReference.invalidateRequest(finalValue);
			Channels.close(localChannelReference.getLocalChannel());
			throw exc;
		}
		return localChannelReference;
	}

	/**
	 * 
	 * @param socketServerAddress
	 * @param isSSL
	 * @return the NetworkChannel
	 * @throws OpenR66ProtocolNetworkException
	 * @throws OpenR66ProtocolRemoteShutdownException
	 * @throws OpenR66ProtocolNoConnectionException
	 */
	private NetworkChannel createNewConnection(SocketAddress socketServerAddress, boolean isSSL)
			throws OpenR66ProtocolNetworkException,
			OpenR66ProtocolRemoteShutdownException,
			OpenR66ProtocolNoConnectionException {
		ReentrantLock socketLock = getChannelLock(socketServerAddress);
		socketLock.lock();
		try {
			if (!isAddressValid(socketServerAddress)) {
				throw new OpenR66ProtocolRemoteShutdownException(
						"Cannot connect to remote server since it is shutting down");
			}
			NetworkChannel networkChannel;
			try {
				networkChannel = getRemoteChannel(socketServerAddress);
			} catch (OpenR66ProtocolNoDataException e1) {
				networkChannel = null;
			}
			if (networkChannel != null) {
				networkChannel.count.incrementAndGet();
				logger.info("Already Connected: {}", networkChannel);
				return networkChannel;
			}
			logger.debug("NEW PHYSICAL CONNECTION REQUIRED");
			ChannelFuture channelFuture = null;
			for (int i = 0; i < Configuration.RETRYNB; i++) {
				try {
					if (isSSL) {
						if (Configuration.configuration.HOST_SSLID != null) {
							channelFuture = clientSslBootstrap.connect(socketServerAddress);
						} else {
							throw new OpenR66ProtocolNoConnectionException("No SSL support");
						}
					} else {
						channelFuture = clientBootstrap.connect(socketServerAddress);
					}
				} catch (ChannelPipelineException e) {
					throw new OpenR66ProtocolNoConnectionException(
							"Cannot connect to remote server due to a channel exception");
				}
				try {
					channelFuture.await();
				} catch (InterruptedException e1) {
				}
				if (channelFuture.isSuccess()) {
					final Channel channel = channelFuture.getChannel();
					if (isSSL) {
						if (!NetworkSslServerHandler.isSslConnectedChannel(channel)) {
							logger.debug("KO CONNECT since SSL handshake is over");
							Channels.close(channel);
							throw new OpenR66ProtocolNoConnectionException(
									"Cannot finish connect to remote server");
						}
					}
					networkChannelGroup.add(channel);
					return putRemoteChannel(channel);
				} else {
					try {
						Thread.sleep(Configuration.WAITFORNETOP);
					} catch (InterruptedException e) {
					}
					if (!channelFuture.isDone()) {
						throw new OpenR66ProtocolNoConnectionException(
								"Cannot connect to remote server due to interruption");
					}
					if (channelFuture.getCause() instanceof ConnectException) {
						logger.debug("KO CONNECT:" +
								channelFuture.getCause().getMessage());
						throw new OpenR66ProtocolNoConnectionException(
								"Cannot connect to remote server", channelFuture
										.getCause());
					} else {
						logger.debug("KO CONNECT but retry", channelFuture
								.getCause());
					}
				}
			}
			throw new OpenR66ProtocolNetworkException(
					"Cannot connect to remote server", channelFuture.getCause());
		} finally {
			socketLock.unlock();
		}
	}

	/**
	 * 
	 * @param channel
	 * @param futureRequest
	 * @return the LocalChannelReference
	 * @throws OpenR66ProtocolNetworkException
	 * @throws OpenR66ProtocolRemoteShutdownException
	 */
	private LocalChannelReference createNewClient(NetworkChannel networkChannel,
			R66Future futureRequest)
			throws OpenR66ProtocolNetworkException,
			OpenR66ProtocolRemoteShutdownException {
		if (!networkChannel.channel.isConnected()) {
			throw new OpenR66ProtocolNetworkException(
					"Network channel no more connected");
		}
		LocalChannelReference localChannelReference = null;
		try {
			localChannelReference = Configuration.configuration
					.getLocalTransaction().createNewClient(networkChannel.channel,
							ChannelUtils.NOCHANNEL, futureRequest);
		} catch (OpenR66ProtocolSystemException e) {
			// check if the channel has other attached local channels
			// NO since done in caller (createConnection)
			/*
			 * logger.info("Try to Close Network"); removeNetworkChannel(networkChannel.channel,
			 * null);
			 */
			throw new OpenR66ProtocolNetworkException(
					"Cannot connect to local channel", e);
		}
		return localChannelReference;
	}

	/**
	 * Create the LocalChannelReference when a remote local channel starts its connection
	 * 
	 * @param channel
	 * @param packet
	 * @return the LocalChannelReference
	 * @throws OpenR66ProtocolRemoteShutdownException
	 * @throws OpenR66ProtocolSystemException
	 */
	public static LocalChannelReference createConnectionFromNetworkChannelStartup(Channel channel,
			NetworkPacket packet)
			throws OpenR66ProtocolRemoteShutdownException, OpenR66ProtocolSystemException {
		NetworkTransaction.addNetworkChannel(channel);
		LocalChannelReference localChannelReference = Configuration.configuration
				.getLocalTransaction().createNewClient(channel,
						packet.getRemoteId(), null);
		return localChannelReference;
	}

	/**
	 * Send a validation of connection with Authentication
	 * 
	 * @param localChannelReference
	 * @throws OpenR66ProtocolNetworkException
	 * @throws OpenR66ProtocolRemoteShutdownException
	 */
	private void sendValidationConnection(
			LocalChannelReference localChannelReference)
			throws OpenR66ProtocolNetworkException,
			OpenR66ProtocolRemoteShutdownException {
		AuthentPacket authent;
		
		try {
			DbHostAuth auth = localChannelReference.getNetworkServerHandler().isSsl() ?
					Configuration.configuration.HOST_SSLAUTH : Configuration.configuration.HOST_AUTH;
			authent = new AuthentPacket(
					Configuration.configuration.getHostId(
							localChannelReference.getNetworkServerHandler().isSsl()),
					FilesystemBasedDigest.passwdCrypt(
							auth.getHostkey()),
					localChannelReference.getLocalId());
		} catch (OpenR66ProtocolNoSslException e1) {
			R66Result finalValue = new R66Result(
					new OpenR66ProtocolSystemException("No SSL support", e1),
					localChannelReference.getSession(), true, ErrorCode.ConnectionImpossible, null);
			logger.error("Authent is Invalid due to no SSL: {}", e1.getMessage());
			if (localChannelReference.getRemoteId().compareTo(ChannelUtils.NOCHANNEL) == 0) {
				ConnectionErrorPacket error = new ConnectionErrorPacket(
						"Cannot connect to localChannel since SSL is not supported", null);
				try {
					ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
				} catch (OpenR66ProtocolPacketException e) {
				}
			}
			localChannelReference.invalidateRequest(finalValue);
			Channels.close(localChannelReference.getLocalChannel());
			throw new OpenR66ProtocolNetworkException(e1);
		}
		logger.debug("Will send request of connection validation");
		localChannelReference.sessionNewState(AUTHENTR);
		try {
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, authent, true);
		} catch (OpenR66ProtocolPacketException e) {
			R66Result finalValue = new R66Result(
					new OpenR66ProtocolSystemException("Wrong Authent Protocol", e),
					localChannelReference.getSession(), true, ErrorCode.ConnectionImpossible, null);
			logger.error("Authent is Invalid due to protocol: {}", e.getMessage());
			localChannelReference.invalidateRequest(finalValue);
			if (localChannelReference.getRemoteId() != ChannelUtils.NOCHANNEL) {
				ConnectionErrorPacket error = new ConnectionErrorPacket(
						"Cannot connect to localChannel since Authent Protocol is invalid", null);
				try {
					ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
				} catch (OpenR66ProtocolPacketException e1) {
				}
			}
			Channels.close(localChannelReference.getLocalChannel());
			throw new OpenR66ProtocolNetworkException("Bad packet", e);
		}
		R66Future future = localChannelReference.getFutureValidateConnection();
		if (future.isFailed()) {
			logger.debug("Will close NETWORK channel since Future cancelled: {}",
					future);
			R66Result finalValue = new R66Result(
					new OpenR66ProtocolSystemException(
							"Out of time or Connection invalid during Authentication"),
					localChannelReference.getSession(), true, ErrorCode.ConnectionImpossible, null);
			logger.warn("Authent is Invalid due to: {} {}", finalValue.exception.getMessage(),
					future.toString());
			localChannelReference.invalidateRequest(finalValue);
			if (localChannelReference.getRemoteId() != ChannelUtils.NOCHANNEL) {
				ConnectionErrorPacket error = new ConnectionErrorPacket(
						"Cannot connect to localChannel with Out of Time", null);
				try {
					ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
				} catch (OpenR66ProtocolPacketException e) {
				}
			}
			Channels.close(localChannelReference.getLocalChannel());
			throw new OpenR66ProtocolNetworkException(
					"Cannot validate connection: " + future.getResult(), future
							.getCause());
		}
	}

	public static ExecutorService getRetrieveExecutor() {
		return retrieveExecutor;
	}

	public static ConcurrentHashMap<Integer, RetrieveRunner> getRetrieveRunnerConcurrentHashMap() {
		return retrieveRunnerConcurrentHashMap;
	}

	/**
	 * Start retrieve operation
	 * 
	 * @param session
	 * @param channel
	 */
	public static void runRetrieve(R66Session session, Channel channel) {
		RetrieveRunner retrieveRunner = new RetrieveRunner(session, channel);
		retrieveRunnerConcurrentHashMap.put(session.getLocalChannelReference().getLocalId(),
				retrieveRunner);
		retrieveRunner.setDaemon(true);
		retrieveExecutor.execute(retrieveRunner);
	}

	/**
	 * Stop a retrieve operation
	 * 
	 * @param localChannelReference
	 */
	public static void stopRetrieve(LocalChannelReference localChannelReference) {
		RetrieveRunner retrieveRunner =
				retrieveRunnerConcurrentHashMap.remove(localChannelReference.getLocalId());
		if (retrieveRunner != null) {
			retrieveRunner.stopRunner();
		}
	}

	/**
	 * Normal end of a Retrieve Operation
	 * 
	 * @param localChannelReference
	 */
	public static void normalEndRetrieve(LocalChannelReference localChannelReference) {
		retrieveRunnerConcurrentHashMap.remove(localChannelReference.getLocalId());
	}

	/**
	 * Stop all Retrieve Executors
	 */
	public static void closeRetrieveExecutors() {
		retrieveExecutor.shutdownNow();
	}

	/**
	 * Close all Network Ttransaction
	 */
	public void closeAll() {
		logger.debug("close All Network Channels");
		try {
			Thread.sleep(Configuration.RETRYINMS * 2);
		} catch (InterruptedException e) {
		}
		if (!Configuration.configuration.isServer) {
			R66ShutdownHook.shutdownHook.launchFinalExit();
		}
		closeRetrieveExecutors();
		networkChannelGroup.close().awaitUninterruptibly();
		clientBootstrap.releaseExternalResources();
		clientSslBootstrap.releaseExternalResources();
		channelClientFactory.releaseExternalResources();
		try {
			Thread.sleep(Configuration.WAITFORNETOP);
		} catch (InterruptedException e) {
		}
		DbAdmin.closeAllConnection();
		Configuration.configuration.clientStop();
		if (! Configuration.configuration.isServer) {
			logger.debug("Last action before exit");
			ChannelUtils.stopLogger();
		}
	}

	/**
	 * 
	 * @param channel
	 * @throws OpenR66ProtocolRemoteShutdownException
	 */
	public static void addNetworkChannel(Channel channel)
			throws OpenR66ProtocolRemoteShutdownException {
		if (!isAddressValid(channel.getRemoteAddress())) {
			throw new OpenR66ProtocolRemoteShutdownException(
					"Channel is already in shutdown");
		}
		try {
			putRemoteChannel(channel);
		} catch (OpenR66ProtocolNoConnectionException e) {
			throw new OpenR66ProtocolRemoteShutdownException(
					"Channel is already in shutdown");
		}
	}

	/**
	 * Add a LocalChannel to a NetworkChannel
	 * 
	 * @param networkChannel
	 * @param localChannel
	 * @throws OpenR66ProtocolRemoteShutdownException
	 */
	public static void addLocalChannelToNetworkChannel(Channel networkChannel,
			Channel localChannel) throws OpenR66ProtocolRemoteShutdownException {
		SocketAddress address = networkChannel.getRemoteAddress();
		if (address != null) {
			NetworkChannel networkChannelObject = networkChannelOnSocketAddressConcurrentHashMap
					.get(address.hashCode());
			if (networkChannelObject != null) {
				networkChannelObject.add(localChannel);
			}
		}
	}

	/**
	 * Shutdown one Network Channel
	 * 
	 * @param channel
	 */
	public static void shuttingdownNetworkChannel(Channel channel) {
		SocketAddress address = channel.getRemoteAddress();
		shuttingdownNetworkChannel(address, channel);
	}

	/**
	 * Shutdown one Network Channel according to its SocketAddress
	 * 
	 * @param address
	 * @param channel
	 *            (can be null)
	 * @return True if the shutdown is starting, False if cannot be done (can be already done
	 *         before)
	 */
	public static boolean shuttingdownNetworkChannel(SocketAddress address, Channel channel) {
		if (address != null) {
			ReentrantLock socketLock = getChannelLock(address);
			socketLock.lock();
			try {
				NetworkChannel networkChannel =
						networkChannelShutdownOnSocketAddressConcurrentHashMap
								.get(address.hashCode());
				if (networkChannel != null) {
					// already done
					logger.debug("Already set as shutdown");
					return false;
				}
				networkChannel = networkChannelOnSocketAddressConcurrentHashMap
						.get(address.hashCode());
				if (networkChannel != null) {
					logger.debug("Set as shutdown");
				} else {
					if (channel != null) {
						logger.debug("Newly Set as shutdown");
						networkChannel = new NetworkChannel(channel);
					}
				}
				if (networkChannel != null) {
					networkChannelShutdownOnSocketAddressConcurrentHashMap.put(address
							.hashCode(), networkChannel);
					if (networkChannel.isShuttingDown) {
						return false;
					}
					networkChannel.shutdownAllLocalChannels();
					R66ShutdownNetworkChannelTimerTask timerTask = new R66ShutdownNetworkChannelTimerTask(
							address.hashCode());
					Configuration.configuration.getTimerClose().newTimeout(timerTask,
							Configuration.configuration.TIMEOUTCON * 3, TimeUnit.MILLISECONDS);
					return true;
				}
			} finally {
				socketLock.unlock();
			}
		}
		return false;
	}

	/**
	 * 
	 * @param channel
	 * @return True if this channel is now blacklisted for a while
	 */
	public static boolean shuttingDownNetworkChannelBlackList(Channel channel) {
		SocketAddress address = channel.getRemoteAddress();
		shuttingdownNetworkChannel(channel);
		if (! Configuration.configuration.blacklistBadAuthent) {
			return false;
		}
		InetSocketAddress inetaddress = (InetSocketAddress) address;
		String remote = inetaddress.getAddress().getHostAddress();
		NetworkChannel networkChannel = networkChannelBlacklistedOnInetSocketAddressConcurrentHashMap.get(remote.hashCode());
		if (networkChannel != null) {
			return false;
		}
		networkChannel = networkChannelShutdownOnSocketAddressConcurrentHashMap.get(address.hashCode());
		if (networkChannel == null) {
			networkChannel = new NetworkChannel(channel);
		}
		R66ShutdownNetworkChannelTimerTask timerTask = new R66ShutdownNetworkChannelTimerTask(remote.hashCode());
		timerTask.isBlacklisted = true;
		Configuration.configuration.getTimerClose().newTimeout(timerTask,
				Configuration.configuration.TIMEOUTCON * 3, TimeUnit.MILLISECONDS);
		networkChannelBlacklistedOnInetSocketAddressConcurrentHashMap.put(remote.hashCode(), networkChannel);
		return true;
	}
	/**
	 * 
	 * @param channel
	 * @return True if this channel is blacklisted
	 */
	public static boolean isBlacklisted(Channel channel) {
		if (! Configuration.configuration.blacklistBadAuthent) {
			return false;
		}
		SocketAddress address = channel.getRemoteAddress();
		InetSocketAddress inetaddress = (InetSocketAddress) address;
		String remote = inetaddress.getAddress().getHostAddress();
		NetworkChannel networkChannel = networkChannelBlacklistedOnInetSocketAddressConcurrentHashMap.get(remote.hashCode());
		return (networkChannel != null);
	}
	
	/**
	 * 
	 * @param address
	 * @return True if this channel is currently in shutdown
	 */
	public static boolean isShuttingdownNetworkChannel(SocketAddress address) {
		ReentrantLock socketLock = getChannelLock(address);
		socketLock.lock();
		try {
			return !isAddressValid(address);
		} finally {
			socketLock.unlock();
		}
	}

	/**
	 * Remove of requester
	 * 
	 * @param requester
	 * @param networkChannel
	 */
	public static void removeClient(String requester, NetworkChannel networkChannel, boolean lock) {
		if (networkChannel != null) {
			if (lock) {
				lockClient.lock();
			}
			try {
				ClientNetworkChannels clientNetworkChannels = remoteClients.get(requester);
				logger.debug("removeClient: remove previous exist? "+(clientNetworkChannels!=null) + " for :"+requester);
				SocketAddress address = networkChannel.channel.getRemoteAddress();
				if (clientNetworkChannels != null) {
					clientNetworkChannels.remove(networkChannel);
					logger.debug("AddClient: remove previous exist? "+(clientNetworkChannels!=null) + " for :"+requester+ " still "+clientNetworkChannels.size());
					if (clientNetworkChannels.isEmpty()) {
						remoteClients.remove(requester);
					}
					if (address != null) {
						remoteClientsPerNetworkChannel.remove(address.hashCode());
					}
				} else if (address != null) {
					clientNetworkChannels = remoteClientsPerNetworkChannel.remove(address.hashCode());
					if (clientNetworkChannels != null) {
						clientNetworkChannels.remove(networkChannel);
						logger.debug("removeClient: remove previous exist? "+(clientNetworkChannels!=null) + " for :"+requester+ " still "+clientNetworkChannels.size());
						if (clientNetworkChannels.isEmpty()) {
							remoteClients.remove(requester);
						}
					}
				}
			} catch (Exception e) {
				logger.error("Bad removeClient", e);
			} finally {
				if (lock) {
					lockClient.unlock();
				}
			}
		}
	}

	/**
	 * Shutdown NetworkChannel as client
	 * 
	 * @param requester
	 * @return NetworkChannel associated with this host as client (only 1 allow even if more are
	 *         available)
	 */
	public static boolean shuttingdownNetworkChannels(String requester) {
		lockClient.lock();
		try {
			ClientNetworkChannels clientNetworkChannels = remoteClients.remove(requester);
			logger.debug("AddClient: shutdown previous exist? "+(clientNetworkChannels!=null) + " for :"+requester);
			if (clientNetworkChannels != null) {
				return clientNetworkChannels.shutdownAll();
			}
		} finally {
			lockClient.unlock();
		}
		return false;
	}

	/**
	 * Add a requester channel (so call only by requested host)
	 * 
	 * @param channel
	 *            (network channel)
	 * @param requester
	 */
	public static void addClient(Channel channel, String requester) {
		SocketAddress address = channel.getRemoteAddress();
		if (address != null) {
			NetworkChannel networkChannel =
					networkChannelOnSocketAddressConcurrentHashMap.get(address.hashCode());
			if (networkChannel != null) {
				lockClient.lock();
				try {
					ClientNetworkChannels prev = remoteClients.putIfAbsent(requester, new ClientNetworkChannels(requester));
					logger.debug("AddClient: add previous exist? "+(prev!=null) + " for "+requester);
					ClientNetworkChannels clientNetworkChannels = remoteClients.get(requester);
					logger.debug("AddClient: add count? "+clientNetworkChannels.size() + " for "+requester);
					clientNetworkChannels.add(networkChannel);
					remoteClientsPerNetworkChannel.put(address.hashCode(), clientNetworkChannels);
				} finally {
					lockClient.unlock();
				}
			}
		}
	}

	/**
	 * 
	 * @param requester
	 * @return The number of NetworkChannels associated with this requester
	 */
	public static int getNumberClients(String requester) {
		ClientNetworkChannels clientNetworkChannels = remoteClients.get(requester);
		if (clientNetworkChannels != null) {
			return clientNetworkChannels.size();
		}
		return 0;
	}

	/**
	 * Force remove of NetworkChannel when it is closed
	 * 
	 * @param address
	 */
	public static void removeForceNetworkChannel(SocketAddress address) {
		if (address == null) {
			return;
		}
		ReentrantLock socketLock = getChannelLock(address);
		socketLock.lock();
		try {
			if (address != null) {
				NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
						.get(address.hashCode());
				if (networkChannel != null) {
					if (networkChannel.isShuttingDown) {
						return;
					}
					logger.debug("NC left: {}", networkChannel);
					networkChannel.shutdownAllLocalChannels();
					networkChannelOnSocketAddressConcurrentHashMap
							.remove(address.hashCode());
				} else {
					logger.debug("Network not registered");
				}
				ClientNetworkChannels clientNetworkChannels = remoteClientsPerNetworkChannel.remove(address.hashCode());
				if (clientNetworkChannels != null) {
					String requester = clientNetworkChannels.getHostId();
					clientNetworkChannels.remove(networkChannel);
					logger.debug("removeClient: remove previous exist? "+(clientNetworkChannels!=null) + " for :"+requester+ " still "+clientNetworkChannels.size());
					if (clientNetworkChannels.isEmpty()) {
						remoteClients.remove(requester);
					}
				}
			}
		} finally {
			socketLock.unlock();
			removeChannelLock(address);
		}
	}

	/**
	 * Class to close the Network Channel if after some delays it has really no Local Channel
	 * attached
	 * 
	 * @author Frederic Bregier
	 * 
	 */
	static class CloseFutureChannel implements TimerTask {

		private static SortedSet<Integer> inCloseRunning =
				Collections
						.synchronizedSortedSet(new TreeSet<Integer>());
		private NetworkChannel networkChannel;
		private String requester;
		private SocketAddress address;

		/**
		 * @param networkChannel
		 * @param requester
		 * @throws OpenR66RunnerErrorException
		 */
		CloseFutureChannel(NetworkChannel networkChannel, SocketAddress address, String requester)
				throws OpenR66RunnerErrorException {
			if (!inCloseRunning.add(networkChannel.channel.getId()))
				throw new OpenR66RunnerErrorException("Already scheduled");
			this.networkChannel = networkChannel;
			this.requester = requester;
			this.address = address;
		}

		public void run(Timeout timeout) throws Exception {
			ReentrantLock socketLock = getChannelLock(address);
			socketLock.lock();
			try {
				logger.debug("NC count: "+networkChannel.count.get()+":"+requester);
				if (networkChannel.count.get() <= 0) {
					long time = networkChannel.lastTimeUsed +
							Configuration.configuration.TIMEOUTCON * 2 -
							System.currentTimeMillis();
					if (time > Configuration.RETRYINMS) {
						// will re execute this request later on
						time = (time / 10) * 10; // round to 10
						Configuration.configuration.getTimerClose().newTimeout(this, time,
								TimeUnit.MILLISECONDS);
						return;
					}
					if (requester != null) {
						NetworkTransaction.removeClient(requester, networkChannel, true);
					} else if (networkChannel.hostId != null) {
						NetworkTransaction.removeClient(networkChannel.hostId, networkChannel, false);
					}
					networkChannelOnSocketAddressConcurrentHashMap
							.remove(address.hashCode());
					logger.info("Will close NETWORK channel");
					WaarpSslUtility.closingSslChannel(networkChannel.channel);
				}
				inCloseRunning.remove(networkChannel.channel.getId());
			} finally {
				socketLock.unlock();
			}
		}

	}

	/**
	 * 
	 * @param channel
	 *            networkChannel
	 * @param localChannel
	 *            localChannel
	 * @param requester
	 *            Requester since call from LocalChannel close (might be null)
	 * @return the number of local channel still connected to this channel
	 */
	public static int removeNetworkChannel(Channel channel, Channel localChannel,
			String requester) {
		SocketAddress address = channel.getRemoteAddress();
		ReentrantLock socketLock = getChannelLock(address);
		socketLock.lock();
		try {
			if (address != null) {
				NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
						.get(address.hashCode());
				if (networkChannel != null) {
					int count = networkChannel.count.decrementAndGet();
					logger.info("Close con: " + networkChannel);
					if (localChannel != null) {
						networkChannel.remove(localChannel);
					}
					if (count <= 0) {
						CloseFutureChannel cfc;
						try {
							cfc = new CloseFutureChannel(networkChannel, address, requester);
							Configuration.configuration.getTimerClose().
									newTimeout(cfc, Configuration.configuration.TIMEOUTCON * 2,
											TimeUnit.MILLISECONDS);
						} catch (OpenR66RunnerErrorException e) {
						}
					}
					logger.debug("NC left: {}", networkChannel);
					return count;
				} else {
					if (channel.isConnected()) {
						logger.debug("Should not be here",
								new OpenR66ProtocolSystemException());
					}
				}
			}
			return 0;
		} finally {
			socketLock.unlock();
		}
	}

	/**
	 * 
	 * @param address
	 * @param host
	 * @return True if a connection is still active on this socket or for this host
	 */
	public static int existConnection(SocketAddress address, String host) {
		logger.debug("exist: "+networkChannelOnSocketAddressConcurrentHashMap.containsKey(address.hashCode())+":"+getNumberClients(host));
		return (networkChannelOnSocketAddressConcurrentHashMap.containsKey(address.hashCode()) ? 1
				: 0)
				+ getNumberClients(host);
	}

	/**
	 * 
	 * @param channel
	 * @return the number of local channel associated with this channel
	 */
	public static int getNbLocalChannel(Channel channel) {
		SocketAddress address = channel.getRemoteAddress();
		if (address != null) {
			NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
					.get(address.hashCode());
			if (networkChannel != null) {
				return networkChannel.count.get();
			}
		}
		return -1;
	}

	/**
	 * 
	 * @param address
	 * @return True if this socket Address is currently valid for connection
	 */
	private static boolean isAddressValid(SocketAddress address) {
		if (R66ShutdownHook.isInShutdown()) {
			logger.debug("IS IN SHUTDOWN");
			return false;
		}
		if (address == null) {
			logger.debug("ADDRESS IS NULL");
			return false;
		}
		try {
			NetworkChannel networkChannel = getRemoteChannel(address);
			logger.debug("IS IN SHUTDOWN: " + networkChannel.isShuttingDown);
			return !networkChannel.isShuttingDown;
		} catch (OpenR66ProtocolRemoteShutdownException e) {
			logger.debug("ALREADY IN SHUTDOWN");
			return false;
		} catch (OpenR66ProtocolNoDataException e) {
			logger.debug("NOT FOUND SO NO SHUTDOWN");
			return true;
		}
	}

	/**
	 * 
	 * @param channel
	 * @return the associated NetworkChannel if any (Null if none)
	 */
	public static NetworkChannel getNetworkChannel(Channel channel) {
		SocketAddress address = channel.getRemoteAddress();
		if (address != null) {
			return networkChannelOnSocketAddressConcurrentHashMap.get(address
					.hashCode());
		}
		return null;
	}

	/**
	 * 
	 * @param address
	 * @return NetworkChannel
	 * @throws OpenR66ProtocolRemoteShutdownException
	 * @throws OpenR66ProtocolNoDataException
	 */
	private static NetworkChannel getRemoteChannel(SocketAddress address)
			throws OpenR66ProtocolRemoteShutdownException,
			OpenR66ProtocolNoDataException {
		if (R66ShutdownHook.isInShutdown()) {
			logger.debug("IS IN SHUTDOWN");
			throw new OpenR66ProtocolRemoteShutdownException(
					"Local Host already in shutdown");
		}
		if (address == null) {
			throw new OpenR66ProtocolRemoteShutdownException(
					"Remote Host already in shutdown");
		}
		int hashCode = address.hashCode();
		NetworkChannel nc = networkChannelShutdownOnSocketAddressConcurrentHashMap
				.get(hashCode);
		if (nc != null) {
			throw new OpenR66ProtocolRemoteShutdownException(
					"Remote Host already in shutdown");
		}
		nc = networkChannelOnSocketAddressConcurrentHashMap.get(hashCode);
		if (nc == null) {
			throw new OpenR66ProtocolNoDataException("Channel not found");
		}
		return nc;
	}

	/**
	 * 
	 * @param channel
	 * @return the associated NetworkChannel
	 * @throws OpenR66ProtocolRemoteShutdownException
	 * @throws OpenR66ProtocolNoDataException
	 */
	private static NetworkChannel putRemoteChannel(Channel channel)
			throws OpenR66ProtocolRemoteShutdownException, OpenR66ProtocolNoConnectionException {
		SocketAddress address = channel.getRemoteAddress();
		if (address != null) {
			NetworkChannel networkChannel;
			try {
				networkChannel = getRemoteChannel(address);
				networkChannel.count.incrementAndGet();
				logger.info("NC active: {}", networkChannel);
				return networkChannel;
			} catch (OpenR66ProtocolRemoteShutdownException e) {
				throw e;
			} catch (OpenR66ProtocolNoDataException e) {
				networkChannel = new NetworkChannel(channel);
				logger.debug("NC new active: {}", networkChannel);
				networkChannelOnSocketAddressConcurrentHashMap.put(address
						.hashCode(), networkChannel);
				return networkChannel;
			}
		}
		throw new OpenR66ProtocolNoConnectionException("Address is not correct");
	}

	/**
	 * To update the last time used
	 * @param networkChannel
	 */
	public static void updateLastTimeUsed(Channel networkChannel) {
		SocketAddress address = networkChannel.getRemoteAddress();
		if (address != null) {
			NetworkChannel nc = networkChannelOnSocketAddressConcurrentHashMap.get(address.hashCode());
			if (nc != null) {
				nc.use();
			}
		}
	}
	
	/**
	 * Check if the last time used is ok with a delay applied to the current time (timeout)
	 * @param networkChannel
	 * @param delay
	 * @return True if OK, else False (should send a KeepAlive)
	 */
	public static boolean checkLastTimeUsed(Channel networkChannel, long delay) {
		SocketAddress address = networkChannel.getRemoteAddress();
		if (address != null) {
			NetworkChannel nc = networkChannelOnSocketAddressConcurrentHashMap.get(address.hashCode());
			if (nc != null) {
				long lastTime = nc.lastTimeUsed;
				logger.debug("CheckLastTime for: "+address.toString()+" "+lastTime+":"+System.currentTimeMillis()+" ok? "+(lastTime + delay > System.currentTimeMillis()));
				if (lastTime + delay > System.currentTimeMillis()) {
					return true;
				}
			}
		}
		return false;
	}
	/**
	 * Remover of Shutdown Remote Host
	 * 
	 * @author Frederic Bregier
	 * 
	 */
	private static class R66ShutdownNetworkChannelTimerTask implements TimerTask {
		/**
		 * href to remove
		 */
		private final int href;
		private boolean isBlacklisted = false;

		/**
		 * Constructor from type
		 * 
		 * @param href
		 */
		public R66ShutdownNetworkChannelTimerTask(int href) {
			super();
			this.href = href;
		}

		public void run(Timeout timeout) throws Exception {
			logger.debug("DEBUG: Will remove shutdown for : " + href);
			if (isBlacklisted) {
				networkChannelBlacklistedOnInetSocketAddressConcurrentHashMap.remove(href);
				return;
			}
			NetworkChannel networkChannel =
					networkChannelShutdownOnSocketAddressConcurrentHashMap.remove(href);
			if (networkChannel != null && networkChannel.channel != null
					&& networkChannel.channel.isConnected()) {
				WaarpSslUtility.closingSslChannel(networkChannel.channel);
			}
		}
	}
}
