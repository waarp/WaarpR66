/**
 *
 */
package openr66.protocol.networkhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.ValidateConnectionPacket;
import openr66.protocol.networkhandler.packet.NetworkPacket;
import openr66.protocol.utils.ChannelUtils;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 * This class handles Network Transaction connections
 *
 * @author frederic bregier
 */
public class NetworkTransaction {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(NetworkTransaction.class);

    static class NetworkChannel {
        AtomicInteger count = new AtomicInteger(1);

        Channel channel;

        public NetworkChannel(Channel channel) {
            this.channel = channel;
        }

        @Override
        public String toString() {
            return "NC: " + channel.isConnected() + " " +
                    channel.getRemoteAddress() + " Count: " + count;
        }
    }

    private static ConcurrentHashMap<Integer, NetworkChannel> networkChannelOnSocketAddressConcurrentHashMap = new ConcurrentHashMap<Integer, NetworkChannel>();

    private static ReentrantLock lock = new ReentrantLock();

    /**
     * ExecutorService Server Boss
     */
    private final ExecutorService execServerBoss = Executors
            .newCachedThreadPool();

    /**
     * ExecutorService Server Worker
     */
    private final ExecutorService execServerWorker = Executors
            .newCachedThreadPool();

    private final ChannelFactory channelClientFactory = new NioClientSocketChannelFactory(
            execServerBoss, execServerWorker, Configuration.configuration.SERVER_THREAD);

    private final ClientBootstrap clientBootstrap = new ClientBootstrap(
            channelClientFactory);

    private final ChannelGroup networkChannelGroup = new DefaultChannelGroup(
            "NetworkChannels");

    public NetworkTransaction() {
        logger.warn("THREAD: " + Configuration.configuration.SERVER_THREAD);
        clientBootstrap.setPipelineFactory(new NetworkServerPipelineFactory());
    }

    public LocalChannelReference createConnection(SocketAddress socketAddress)
            throws OpenR66ProtocolNetworkException {
        lock.lock();
        try {
            Channel channel = createNewConnection(socketAddress);
            LocalChannelReference localChannelReference = createNewClient(channel);
            sendValidationConnection(localChannelReference);
            return localChannelReference;
        } finally {
            lock.unlock();
        }
    }

    private Channel createNewConnection(SocketAddress socketServerAddress)
            throws OpenR66ProtocolNetworkException {
        NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
                .get(socketServerAddress.hashCode());
        if (networkChannel != null) {
            if (networkChannel.channel.isConnected()) {
                logger.info("Already Connected: " + networkChannel.toString());
                return networkChannel.channel;
            }
        }
        ChannelFuture channelFuture = null;
        for (int i = 0; i < Configuration.RETRYNB; i ++) {
            channelFuture = clientBootstrap.connect(socketServerAddress);
            channelFuture.awaitUninterruptibly();
            if (channelFuture.isSuccess()) {
                final Channel channel = channelFuture.getChannel();
                networkChannelGroup.add(channel);
                if (networkChannel != null) {
                    networkChannel.channel = channel;
                }
                return channel;
            }
            try {
                Thread.sleep(Configuration.RETRYINMS);
            } catch (InterruptedException e) {
                throw new OpenR66ProtocolNetworkException(
                        "Cannot connect to remote server", e);
            }
        }
        throw new OpenR66ProtocolNetworkException(
                "Cannot connect to remote server", channelFuture.getCause());
    }

    private LocalChannelReference createNewClient(Channel channel)
            throws OpenR66ProtocolNetworkException {
        if (!channel.isConnected()) {
            throw new OpenR66ProtocolNetworkException(
                    "Network channel no more connected");
        }
        LocalChannelReference localChannelReference = null;
        try {
            localChannelReference = Configuration.configuration
                    .getLocalTransaction().createNewClient(channel,
                            ChannelUtils.NOCHANNEL);
        } catch (OpenR66ProtocolSystemException e) {
            throw new OpenR66ProtocolNetworkException(
                    "Cannot connect to local channel", e);
        }
        NetworkTransaction.addNetworkChannel(channel);
        return localChannelReference;
    }

    private void sendValidationConnection(
            LocalChannelReference localChannelReference)
            throws OpenR66ProtocolNetworkException {
        ValidateConnectionPacket validate = new ValidateConnectionPacket(
                localChannelReference.getLocalId());
        NetworkPacket packet;
        try {
            packet = new NetworkPacket(localChannelReference.getLocalId(),
                    localChannelReference.getRemoteId(), validate.getType(),
                    validate.getLocalPacket());
        } catch (OpenR66ProtocolPacketException e) {
            throw new OpenR66ProtocolNetworkException("Bad packet", e);
        }
        Channel channel = localChannelReference.getNetworkChannel();
        if (!channel.isConnected()) {
            throw new OpenR66ProtocolNetworkException(
                    "Cannot validate connection since connection closed");
        }
        ChannelUtils.write(localChannelReference.getNetworkChannel(), packet)
                .awaitUninterruptibly();
        if (!localChannelReference.getValidation()) {
            Channels.close(channel);
            throw new OpenR66ProtocolNetworkException(
                    "Cannot validate connection");
        }
    }

    public void closeAll() {
        logger.warn("close All Network Channels");
        networkChannelGroup.close().awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
        channelClientFactory.releaseExternalResources();
    }

    public static void addNetworkChannel(Channel channel) {
        lock.lock();
        try {
            NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
                    .get(channel.getRemoteAddress().hashCode());
            if (networkChannel != null) {
                networkChannel.count.incrementAndGet();
                logger.info("NC active: " + networkChannel.toString());
            } else {
                networkChannel = new NetworkChannel(channel);
                networkChannelOnSocketAddressConcurrentHashMap.put(channel
                        .getRemoteAddress().hashCode(), networkChannel);
            }
        } finally {
            lock.unlock();
        }
    }

    public static int removeNetworkChannel(Channel channel) {
        lock.lock();
        try {
            NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
                    .get(channel.getRemoteAddress().hashCode());
            if (networkChannel != null) {
                if (networkChannel.count.decrementAndGet() <= 0) {
                    // networkChannel.count.set(0);
                    networkChannelOnSocketAddressConcurrentHashMap
                            .remove(channel.getRemoteAddress().hashCode());
                    logger.info("Close network channel");
                    Channels.close(channel).awaitUninterruptibly();
                    return 0;
                }
                logger.info("NC left: " + networkChannel.toString());
                return networkChannel.count.get();
            } else {
                if (channel.isConnected()) {
                    logger.error("Should not be here",
                            new OpenR66ProtocolSystemException());
                    // Channels.close(channel);
                }
            }
            return 0;
        } finally {
            lock.unlock();
        }
    }

    public static int getNbLocalChannel(Channel channel) {
        NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
                .get(channel.getRemoteAddress().hashCode());
        if (networkChannel != null) {
            return networkChannel.count.get();
        }
        return -1;
    }
}
