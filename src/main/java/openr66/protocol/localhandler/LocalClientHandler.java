/**
 * 
 */
package openr66.protocol.localhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.protocol.config.Configuration;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.packet.AbstractLocalPacket;
import openr66.protocol.packet.ErrorPacket;
import openr66.protocol.packet.NetworkPacket;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * @author frederic bregier
 * 
 */
@ChannelPipelineCoverage("one")
public class LocalClientHandler extends SimpleChannelHandler {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(LocalClientHandler.class);
    
    private LocalChannelReference localChannelReference = null;
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#channelClosed(org.jboss.
     * netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        // FIXME nothing to do ?
        logger.info("Local Client Channel Closed: "+e.getChannel().getId());
        if (this.localChannelReference != null) {
            NetworkTransaction.configuration.getLocalTransaction().removeFromId(this.localChannelReference.getId());
            this.localChannelReference = null;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#channelConnected(org.jboss
     * .netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        // FIXME once connected, first message should create the LocalChannelReference (not now)
        logger.info("Local Client Channel Connected: "+e.getChannel().getId());
    }

    private void initLocalClientHandler(Channel channel) throws InterruptedException {
        while (this.localChannelReference == null) {
            Thread.sleep(Configuration.RETRYINMS);
            this.localChannelReference = 
                NetworkTransaction.configuration.getLocalTransaction().getFromId(channel.getId());
        }
    }
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#messageReceived(org.jboss
     * .netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.MessageEvent)
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {
        if (this.localChannelReference == null) {
            this.initLocalClientHandler(e.getChannel());
        }
        // FIXME now handle message from local server and write back them the network channel
        AbstractLocalPacket packet = (AbstractLocalPacket) e.getMessage();
        logger.info("Local Client Channel Recv: "+e.getChannel().getId());
        // FIXME write back to the network channel
        NetworkPacket networkPacket = new NetworkPacket(this.localChannelReference.getId(),
                this.localChannelReference.getRemoteId(),packet.getLocalPacket());
        Channels.write(this.localChannelReference.getNetworkChannel(), networkPacket);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#exceptionCaught(org.jboss
     * .netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ExceptionEvent)
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {
        // FIXME informs network of the problem
        logger.error("Local Client Channel Exception: "+e.getChannel().getId(),e.getCause());
        if (this.localChannelReference != null) {
            ErrorPacket errorPacket = new ErrorPacket(e.getCause().getMessage(),null,null);
            NetworkPacket networkPacket = new NetworkPacket(this.localChannelReference.getId(),
                    this.localChannelReference.getRemoteId(),errorPacket.getLocalPacket());
            Channels.write(this.localChannelReference.getNetworkChannel(), networkPacket);
        }
        Channels.close(e.getChannel());
    }

}
