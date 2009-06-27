/**
 * 
 */
package openr66.protocol.localhandler;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.jboss.netty.channel.local.LocalAddress;

/**
 * This class handles Local Transaction connections
 * 
 * @author frederic bregier
 * 
 */
public class LocalTransaction {
    private final ChannelFactory channelClientFactory = new DefaultLocalClientChannelFactory();

    private final ChannelFactory channelServerFactory = new DefaultLocalServerChannelFactory();

    private final ClientBootstrap clientBootstrap = new ClientBootstrap(
            channelClientFactory);

    private final ServerBootstrap serverBootstrap = new ServerBootstrap(
            channelServerFactory);

    private final Channel serverChannel;

    private final LocalAddress socketServerAddress = new LocalAddress("0");

    private final ChannelGroup localChannelGroup = new DefaultChannelGroup(
            "LocalChannels");

    public LocalTransaction(Channel networkChannel) {
        serverBootstrap.setPipelineFactory(new LocalServerPipelineFactory(
                networkChannel));
        serverChannel = serverBootstrap.bind(socketServerAddress);
        localChannelGroup.add(serverChannel);
        clientBootstrap.setPipelineFactory(new LocalClientPipelineFactory());
    }

    public Channel createNewClient() {
        ChannelFuture channelFuture = clientBootstrap
                .connect(socketServerAddress);
        channelFuture.awaitUninterruptibly();
        Channel channel = channelFuture.getChannel();
        localChannelGroup.add(channel);
        return channel;
    }

    public void clientChannelClose(Channel channel) {
        channel.close();
        channel.getCloseFuture().awaitUninterruptibly();
    }

    public void closeAll() {
        localChannelGroup.close().awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
        channelClientFactory.releaseExternalResources();
        serverBootstrap.releaseExternalResources();
        channelServerFactory.releaseExternalResources();
    }
}
