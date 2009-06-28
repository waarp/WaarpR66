/**
 * 
 */
package openr66.protocol.networkhandler;

import goldengate.common.future.GgFuture;

import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import openr66.protocol.config.Configuration;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * This class handles Network Transaction connections
 * 
 * @author frederic bregier
 * 
 */
public class NetworkTransaction {
    /**
     * General Configuration object
     */
    public static final Configuration configuration = new Configuration();
    
    /**
     * ExecutorService Server Boss
     */
    private final ExecutorService execServerBoss = Executors.newCachedThreadPool();

    /**
     * ExecutorService Server Worker
     */
    private final ExecutorService execServerWorker = Executors.newCachedThreadPool();
    
    private final ChannelFactory channelClientFactory = new NioClientSocketChannelFactory(execServerBoss,
            execServerWorker, Configuration.SERVER_THREAD);

    private final ClientBootstrap clientBootstrap = new ClientBootstrap(
            channelClientFactory);
    
    private final ChannelGroup networkChannelGroup = new DefaultChannelGroup(
            "NetworkChannels");

    public NetworkTransaction() {
        clientBootstrap.setPipelineFactory(new NetworkServerPipelineFactory());
    }

    public Channel createNewClient(SocketAddress socketServerAddress) {
        ChannelFuture channelFuture = clientBootstrap
                .connect(socketServerAddress);
        channelFuture.awaitUninterruptibly();
        Channel channel = channelFuture.getChannel();
        networkChannelGroup.add(channel);
        
        return channel;
    }

    public void closeAll() {
        networkChannelGroup.close().awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
        channelClientFactory.releaseExternalResources();
    }
}
