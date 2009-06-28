/**
 * 
 */
package openr66.protocol.localhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import openr66.protocol.exception.OpenR66ProtocolShutdownException;
import openr66.protocol.packet.AbstractLocalPacket;
import openr66.protocol.packet.ErrorPacket;
import openr66.protocol.packet.TestPacket;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * The local server handler handles real end file operations.
 * @author frederic bregier
 * 
 */
@ChannelPipelineCoverage("one")
public class LocalServerHandler extends SimpleChannelHandler {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(LocalServerHandler.class);
    
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
        logger.info("Local Server Channel Closed: "+e.getChannel().getId());
        // FIXME clean session objects like files
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
        logger.info("Local Server Channel Connected: "+e.getChannel().getId());
        // FIXME prepare session objects
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
        logger.info("Local Server Channel Recv: "+e.getChannel().getId());
        // FIXME action as requested and answer if necessary
        AbstractLocalPacket packet = (AbstractLocalPacket) e.getMessage();
        if (packet instanceof TestPacket) {
            logger.info(e.getChannel().getId()+": "+((TestPacket) packet).toString());
            // simply write back after+1
            ((TestPacket) packet).update();
            Channels.write(e.getChannel(), packet);
        } else if (packet instanceof ErrorPacket) {
            logger.warn(e.getChannel().getId()+": "+((ErrorPacket) packet).toString());
        } else {
            logger.error("Unknown Mesg");
        }
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
        // FIXME inform clients
        logger.error("Local Server Channel Exception: "+e.getChannel().getId(), e.getCause());
    }

}
