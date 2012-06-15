/**
 * This file is part of GoldenGate Project (named also GoldenGate or GG).
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 * 
 * All GoldenGate Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * 
 * GoldenGate is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * GoldenGate . If not, see <http://www.gnu.org/licenses/>.
 */
package openr66.protocol.localhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import openr66.context.ErrorCode;
import openr66.context.R66FiniteDualStates;
import openr66.context.R66Result;
import openr66.context.R66Session;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;
import openr66.protocol.exception.OpenR66ProtocolShutdownException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
import openr66.protocol.localhandler.packet.StartupPacket;
import openr66.protocol.localhandler.packet.ValidPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.networkhandler.packet.NetworkPacket;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.jboss.netty.channel.local.LocalAddress;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;

/**
 * This class handles Local Transaction connections
 * 
 * @author frederic bregier
 */
public class LocalTransaction {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(LocalTransaction.class);

    /**
     * HashMap of LocalChannelReference using LocalChannelId
     */
    final ConcurrentHashMap<Integer, LocalChannelReference> localChannelHashMap = new ConcurrentHashMap<Integer, LocalChannelReference>();

    /**
     * HashMap of Validation of LocalChannelReference using LocalChannelId
     */
    final ConcurrentHashMap<Integer, R66Future> validLocalChannelHashMap = new ConcurrentHashMap<Integer, R66Future>();

    /**
     * HashMap of LocalChannelReference using requested_requester_specialId
     */
    final ConcurrentHashMap<String, LocalChannelReference> localChannelHashMapExternal = new ConcurrentHashMap<String, LocalChannelReference>();

    /**
     * Remover from HashMap
     */
    private final ChannelFutureListener remover = new ChannelFutureListener() {
        public void operationComplete(ChannelFuture future) {
            remove(future.getChannel());
        }
    };

    private final ChannelFactory channelServerFactory = new DefaultLocalServerChannelFactory();

    private final ServerBootstrap serverBootstrap = new ServerBootstrap(
            channelServerFactory);

    private final Channel serverChannel;

    private final LocalAddress socketLocalServerAddress = new LocalAddress("0");

    private final ChannelFactory channelClientFactory = new DefaultLocalClientChannelFactory();

    private final ClientBootstrap clientBootstrap = new ClientBootstrap(
            channelClientFactory);

    private final ChannelGroup localChannelGroup = new DefaultChannelGroup(
            "LocalChannels");

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

    /**
     * Get the corresponding LocalChannelReference
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
            if (localChannelReference.getRemoteId() != remoteId) {
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
     * @param networkChannel
     * @param remoteId
     * @param futureRequest
     * @return the LocalChannelReference
     * @throws OpenR66ProtocolSystemException
     */
    public LocalChannelReference createNewClient(Channel networkChannel,
            Integer remoteId, R66Future futureRequest)
            throws OpenR66ProtocolSystemException {
        ChannelFuture channelFuture = null;
        logger.debug("Status LocalChannelServer: {} {}", serverChannel
                .getClass().getName(), serverChannel.getConfig()
                .getConnectTimeoutMillis() + " " + serverChannel.isBound());
        R66Future validLCR = new R66Future(true);
        validLocalChannelHashMap.put(remoteId, validLCR);
        for (int i = 0; i < Configuration.RETRYNB; i ++) {
            channelFuture = clientBootstrap.connect(socketLocalServerAddress);
            try {
                channelFuture.await();
            } catch (InterruptedException e1) {
                validLCR.cancel();
                validLocalChannelHashMap.remove(remoteId);
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
                final LocalChannelReference localChannelReference = new LocalChannelReference(
                        channel, networkChannel, remoteId, futureRequest);
                logger.debug("Create LocalChannel entry: " + i + " {}",
                        localChannelReference);
                channel.getCloseFuture().addListener(remover);
                localChannelHashMap.put(channel.getId(), localChannelReference);
                try {
                    NetworkTransaction.addLocalChannelToNetworkChannel(
                            networkChannel, channel);
                } catch (OpenR66ProtocolRemoteShutdownException e) {
                    validLCR.cancel();
                    validLocalChannelHashMap.remove(remoteId);
                    Channels.close(channel);
                    throw new OpenR66ProtocolSystemException(
                            "Cannot connect to local handler", e);
                }
                // Now send first a Startup message
                StartupPacket startup = new StartupPacket(
                        localChannelReference.getLocalId());
                try {
                    Channels.write(channel, startup).await();
                } catch (InterruptedException e) {
                    logger.error("Can't connect to local server due to interruption" + i);
                    validLCR.cancel();
                    validLocalChannelHashMap.remove(remoteId);
                    throw new OpenR66ProtocolSystemException(
                            "Cannot connect to local handler", e);
                }
                validLCR.setSuccess();
                return localChannelReference;
            } else {
                logger.error("Can't connect to local server " + i);
            }
            try {
                Thread.sleep(Configuration.RETRYINMS);
            } catch (InterruptedException e) {
                validLCR.cancel();
                validLocalChannelHashMap.remove(remoteId);
                throw new OpenR66ProtocolSystemException(
                        "Cannot connect to local handler", e);
            }
        }
        validLCR.cancel();
        validLocalChannelHashMap.remove(remoteId);
        logger.error("LocalChannelServer: " +
                serverChannel.getClass().getName() + " " +
                serverChannel.getConfig().getConnectTimeoutMillis() + " " +
                serverChannel.isBound());
        throw new OpenR66ProtocolSystemException(
                "Cannot connect to local handler: " + socketLocalServerAddress +
                        " " + serverChannel.isBound() + " " + serverChannel,
                channelFuture.getCause());
    }

    /**
     * 
     * @param id
     * @return the LocalChannelReference
     */
    public LocalChannelReference getFromId(Integer id) {
        for (int i = 0; i < Configuration.RETRYNB * 4; i ++) {
            LocalChannelReference lcr = localChannelHashMap.get(id);
            if (lcr == null) {
                /*
                 * R66Future future = validLocalChannelHashMap.get(id);
                 * logger.debug("DEBUG Future ValidLocalChannel: not found: " +
                 * id + (future != null)); if (future != null) { try {
                 * future.await(Configuration.configuration.TIMEOUTCON); } catch
                 * (InterruptedException e) { return
                 * localChannelHashMap.get(id); }
                 * logger.debug("DEBUG Future ValidLocalChannel: " + id +
                 * future.isDone() + ":" + future.isSuccess()); if
                 * (future.isSuccess()) { return localChannelHashMap.get(id); }
                 * else if (future.isFailed()) { return null; } } else {
                 * logger.debug("DEBUG Future ValidLocalChannel: Sleep" + id);
                 * try { Thread.sleep(Configuration.RETRYINMS); } catch
                 * (InterruptedException e) { } continue; }
                 */
                try {
                    Thread.sleep(Configuration.RETRYINMS * 2);
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
     * @param channel
     */
    public void remove(Channel channel) {
        LocalChannelReference localChannelReference = localChannelHashMap
                .remove(channel.getId());
        if (localChannelReference != null) {
            logger.debug("Remove LocalChannel");
            R66Future validLCR = validLocalChannelHashMap
                    .remove(localChannelReference.getRemoteId());
            if (validLCR != null) {
                validLCR.cancel();
            }
            DbTaskRunner runner = null;
            if (localChannelReference.getSession() != null) {
                runner = localChannelReference.getSession().getRunner();
            }
            R66Result result = new R66Result(
                    new OpenR66ProtocolSystemException(
                            "While closing Local Channel"),
                    localChannelReference.getSession(), false,
                    ErrorCode.ConnectionImpossible, runner);
            localChannelReference.validateConnection(false, result);
            if (localChannelReference.getSession() != null) {
                if (runner != null) {
                    String key = runner.getKey();
                    localChannelHashMapExternal.remove(key);
                }
            }
        }
    }

    /**
     * 
     * @param runner
     * @param lcr
     */
    public void setFromId(DbTaskRunner runner, LocalChannelReference lcr) {
        String key = runner.getKey();
        localChannelHashMapExternal.put(key, lcr);
    }

    /**
     * 
     * @param key
     *            as "requested requester specialId"
     * @return the LocalChannelReference
     */
    public LocalChannelReference getFromRequest(String key) {
        return localChannelHashMapExternal.get(key);
    }

    /**
     * 
     * @return the number of active local channels
     */
    public int getNumberLocalChannel() {
        return localChannelHashMap.size();
    }

    private static class CloseLocalChannelsFromNetworkChannelTast implements TimerTask {

        LocalTransaction localTransaction;
        AtomicInteger semaphore;
        LocalChannelReference localChannelReference;
        boolean analysis;
        
        public CloseLocalChannelsFromNetworkChannelTast(
                LocalTransaction localTransaction,
                AtomicInteger semaphore,
                LocalChannelReference localChannelReference) {
            this.localTransaction = localTransaction;
            this.semaphore = semaphore;
            this.localChannelReference = localChannelReference;
            analysis = true;
        }
        
        public void run(Timeout timeout) {
            // give a chance for the LocalChannel to stop normally
            if (analysis) {
                boolean wait = false;
                if (!localChannelReference.getFutureRequest().isDone()) {
                    if (localChannelReference.getFutureValidRequest().isDone() &&
                            localChannelReference.getFutureValidRequest()
                                    .isFailed()) {
                        logger.debug("Already currently on finalize");
                        wait = true;
                    } else {
                        R66Result finalValue = new R66Result(
                                localChannelReference.getSession(), true,
                                ErrorCode.Shutdown, null);
                        if (localChannelReference.getSession() != null) {
                            try {
                                localChannelReference.getSession()
                                        .tryFinalizeRequest(finalValue);
                            } catch (OpenR66RunnerErrorException e) {
                            } catch (OpenR66ProtocolSystemException e) {
                            }
                        }
                    }
                }
                if (wait) {
                    this.analysis = false;
                    Configuration.configuration.getTimerClose().newTimeout(this, 
                            Configuration.RETRYINMS * 10, TimeUnit.MILLISECONDS);
                    return;
                }
            }
            logger.debug("Will close local channel");
            try {
                Channels.close(localChannelReference.getLocalChannel()).await();
            } catch (InterruptedException e) {
            }
            localTransaction.remove(localChannelReference.getLocalChannel());
            semaphore.decrementAndGet();
        }
        
    }
    /**
     * Close all Local Channels from the NetworkChannel
     * 
     * @param networkChannel
     */
    public void closeLocalChannelsFromNetworkChannel(Channel networkChannel) {
        Collection<LocalChannelReference> collection = localChannelHashMap
                .values();
        AtomicInteger semaphore = new AtomicInteger();
        Iterator<LocalChannelReference> iterator = collection.iterator();
        while (iterator.hasNext()) {
            LocalChannelReference localChannelReference = iterator.next();
            if (localChannelReference.getNetworkChannel().compareTo(
                    networkChannel) == 0) {
                semaphore.incrementAndGet();
                CloseLocalChannelsFromNetworkChannelTast task = 
                    new CloseLocalChannelsFromNetworkChannelTast(this, 
                            semaphore, localChannelReference);
                Configuration.configuration.getTimerClose().newTimeout(task, 
                        Configuration.RETRYINMS * 10, TimeUnit.MILLISECONDS);
            }
        }
        while (true) {
            if (semaphore.get() == 0) {
                break;
            }
            try {
                Thread.sleep(Configuration.RETRYINMS*2);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    /**
     * Debug function (while shutdown for instance)
     */
    public void debugPrintActiveLocalChannels() {
        Collection<LocalChannelReference> collection = localChannelHashMap
                .values();
        Iterator<LocalChannelReference> iterator = collection.iterator();
        while (iterator.hasNext()) {
            LocalChannelReference localChannelReference = iterator.next();
            logger.debug("Will close local channel: {}", localChannelReference);
            logger.debug(
                    " Containing: {}",
                    (localChannelReference.getSession() != null? localChannelReference
                            .getSession() : "no session"));
        }
    }

    /**
     * Informs all remote client that the server is shutting down
     */
    public void shutdownLocalChannels() {
        Collection<LocalChannelReference> collection = localChannelHashMap
                .values();
        Iterator<LocalChannelReference> iterator = collection.iterator();
        ValidPacket packet = new ValidPacket("Shutdown forced", null,
                LocalPacketFactory.SHUTDOWNPACKET);
        ChannelBuffer buffer = null;
        while (iterator.hasNext()) {
            LocalChannelReference localChannelReference = iterator.next();
            logger.debug("Inform Shutdown {}", localChannelReference);
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
                    R66Result result = new R66Result(
                            new OpenR66ProtocolShutdownException(), session,
                            true, ErrorCode.Shutdown, runner);
                    result.other = packet;
                    try {
                        buffer = packet.getLocalPacket();
                    } catch (OpenR66ProtocolPacketException e1) {
                    }
                    localChannelReference
                            .sessionNewState(R66FiniteDualStates.SHUTDOWN);
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
                buffer = packet.getLocalPacket();
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
