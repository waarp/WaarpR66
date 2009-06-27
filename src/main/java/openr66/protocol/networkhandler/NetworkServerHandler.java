/**
 * 
 */
package openr66.protocol.networkhandler;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * @author fbregier
 * 
 */
public class NetworkServerHandler extends SimpleChannelHandler {

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
        // TODO Auto-generated method stub
        super.channelConnected(ctx, e);
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
        // TODO Auto-generated method stub
        // Chaque message recu devra etre prefixe de channelId destinataire,
        // channelId source(localchannel Id)
        super.messageReceived(ctx, e);
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
