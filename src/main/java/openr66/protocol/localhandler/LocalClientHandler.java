/**
 * 
 */
package openr66.protocol.localhandler;

import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.packet.AbstractLocalPacket;
import openr66.protocol.packet.NetworkPacket;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;

/**
 * @author fbregier
 * 
 */
public class LocalClientHandler extends SimpleChannelHandler {

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
        // TODO Auto-generated method stub
        super.channelClosed(ctx, e);
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
        // FIXME once connected, should get the LocalChannelReference (not now)
        // TODO Auto-generated method stub
        super.channelConnected(ctx, e);
    }

    private void initLocalClientHandler(Channel channel) {
        this.localChannelReference = NetworkTransaction.configuration.getLocalTransaction().getFromId(channel.getId());
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
        // FIXME write back to the network channel
        NetworkPacket networkPacket = new NetworkPacket(this.localChannelReference.getId(),
                this.localChannelReference.getRemoteId(),packet.getLocalPacket());
        Channels.write(this.localChannelReference.getNetworkChannel(), networkPacket);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#writeComplete(org.jboss.
     * netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.WriteCompletionEvent)
     */
    @Override
    public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e)
            throws Exception {
        // TODO Auto-generated method stub
        super.writeComplete(ctx, e);
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
        // TODO Auto-generated method stub
        super.exceptionCaught(ctx, e);
    }

}
