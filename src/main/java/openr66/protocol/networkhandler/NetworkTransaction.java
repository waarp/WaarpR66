/**
 * 
 */
package openr66.protocol.networkhandler;

import java.net.SocketAddress;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

/**
 * This class handles Network Transaction connections
 * 
 * @author frederic bregier
 * 
 */
public class NetworkTransaction {
    private final ChannelFactory channelClientFactory = null;

    private final ChannelFactory channelServerFactory = null;

    private final ClientBootstrap clientBootstrap = new ClientBootstrap(
            channelClientFactory);

    private final ServerBootstrap serverBootstrap = new ServerBootstrap(
            channelServerFactory);

    private final Channel serverChannel;

    private final ChannelGroup networkChannelGroup = new DefaultChannelGroup(
            "NetworkChannels");

    public NetworkTransaction(SocketAddress socketServerAddress) {
        serverBootstrap.setPipelineFactory(new NetworkServerPipelineFactory());
        serverChannel = serverBootstrap.bind(socketServerAddress);
        networkChannelGroup.add(serverChannel);
        clientBootstrap.setPipelineFactory(new NetworkClientPipelineFactory());
    }

    public Channel createNewClient(SocketAddress socketServerAddress) {
        ChannelFuture channelFuture = clientBootstrap
                .connect(socketServerAddress);
        channelFuture.awaitUninterruptibly();
        Channel channel = channelFuture.getChannel();
        networkChannelGroup.add(channel);
        return channel;
    }

    public void clientChannelClose(Channel channel) {
        channel.close();
        channel.getCloseFuture().awaitUninterruptibly();
    }

    public void closeAll() {
        networkChannelGroup.close().awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
        channelClientFactory.releaseExternalResources();
        serverBootstrap.releaseExternalResources();
        channelServerFactory.releaseExternalResources();
    }
}
