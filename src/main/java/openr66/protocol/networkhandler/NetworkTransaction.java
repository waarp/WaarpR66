/**
 * 
 */
package openr66.protocol.networkhandler;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;

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
    static class NetworkChannel {
        AtomicInteger count = new AtomicInteger(1);
        Channel channel;
        public NetworkChannel(Channel channel) {
            this.channel = channel;
        }
    }
    private static ConcurrentHashMap<Integer, NetworkChannel> networkChannelConcurrentHashMap =
        new ConcurrentHashMap<Integer, NetworkChannel>();
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
            execServerBoss, execServerWorker, Configuration.SERVER_THREAD);

    private final ClientBootstrap clientBootstrap = new ClientBootstrap(
            channelClientFactory);

    private final ChannelGroup networkChannelGroup = new DefaultChannelGroup(
            "NetworkChannels");

    public NetworkTransaction() {
        clientBootstrap.setPipelineFactory(new NetworkServerPipelineFactory());
    }

    public Channel createNewClient(SocketAddress socketServerAddress) throws OpenR66ProtocolNetworkException {
        ChannelFuture channelFuture = null;
        for (int i = 0; i < Configuration.RETRYNB; i++) {
            channelFuture = clientBootstrap
                    .connect(socketServerAddress);
            channelFuture.awaitUninterruptibly();
            if (channelFuture.isSuccess()) {
                final Channel channel = channelFuture.getChannel();
                networkChannelGroup.add(channel);
                return channel;
            }
            try {
                Thread.sleep(Configuration.RETRYINMS);
            } catch (InterruptedException e) {
                throw new OpenR66ProtocolNetworkException("Cannot connect to remote server",e);
            }
        }
        throw new OpenR66ProtocolNetworkException("Cannot connect to remote server", channelFuture.getCause());
    }

    public void closeAll() {
        networkChannelGroup.close().awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
        channelClientFactory.releaseExternalResources();
    }
    public static void addNetworkChannel(Channel channel) {
        lock.lock();
        try {
            NetworkChannel networkChannel = networkChannelConcurrentHashMap.get(channel.getId());
            if (networkChannel != null) {
                networkChannel.count.incrementAndGet();
            } else {
                networkChannel = new NetworkChannel(channel);
                networkChannelConcurrentHashMap.put(channel.getId(), networkChannel);
            }
        } finally {
            lock.unlock();
        }
    }
    public static void removeNetworkChannel(Channel channel) {
        lock.lock();
        try {
            NetworkChannel networkChannel = networkChannelConcurrentHashMap.get(channel.getId());
            if (networkChannel != null) {
                if (networkChannel.count.decrementAndGet() == 0) {
                    networkChannelConcurrentHashMap.remove(channel.getId());
                    Channels.close(channel);
                }
            } else {
                Channels.close(channel);
            }
        } finally {
            lock.unlock();
        }
    }
}
