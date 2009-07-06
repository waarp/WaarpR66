/**
 * 
 */
package openr66.protocol.localhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ExceptionTrappedFactory;
import openr66.protocol.exception.OpenR66ProtocolException;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.exception.OpenR66ProtocolShutdownException;
import openr66.protocol.localhandler.packet.AbstractLocalPacket;
import openr66.protocol.localhandler.packet.ErrorPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.networkhandler.packet.NetworkPacket;
import openr66.protocol.utils.ChannelUtils;

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
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#channelClosed(org.jboss.
     * netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        logger.warn("Local Client Channel Closed: " + e.getChannel().getId());
        if (localChannelReference != null) {
            logger.info("Will close Network channel");
            NetworkTransaction.removeNetworkChannel(localChannelReference.getNetworkChannel());
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#channelConnected(org.jboss
     * .netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        // FIXME once connected, first message should create the
        // LocalChannelReference (not now)
        logger
                .info("Local Client Channel Connected: "
                        + e.getChannel().getId());
    }

    private void initLocalClientHandler(Channel channel)
            throws InterruptedException, OpenR66ProtocolNetworkException {
        int i = 0;
        if (localChannelReference == null) {
            for (i = 0; i < Configuration.RETRYNB*10; i++, Thread.sleep(Configuration.RETRYINMS)) {
                localChannelReference = Configuration.configuration
                    .getLocalTransaction().getFromId(channel.getId());
                if (localChannelReference != null) {
                    return;
                }
            }
            throw new OpenR66ProtocolNetworkException("Cannot find local connection");            
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#messageReceived(org.jboss
     * .netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.MessageEvent)
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {
        if (localChannelReference == null) {
            initLocalClientHandler(e.getChannel());
        }
        // FIXME now handle message from local server and write back them the
        // network channel
        final AbstractLocalPacket packet = (AbstractLocalPacket) e.getMessage();
        logger.info("Local Client Channel Recv: " + e.getChannel().getId());
        // FIXME write back to the network channel
        if (packet instanceof ErrorPacket) {
            ErrorPacket errorPacket = (ErrorPacket) packet;
            if (errorPacket.getCode() == ErrorPacket.CLOSECODE) {
                logger.info("Will close channel");
                if (!localChannelReference.getFuture().isDone()) {
                    localChannelReference.getFuture().setFailure(null);
                }
                Channels.close(e.getChannel());
                return;
            } else if (errorPacket.getCode() == ErrorPacket.IGNORECODE) {
                return;
            }
            final NetworkPacket networkPacket = new NetworkPacket(
                    localChannelReference.getLocalId(), localChannelReference
                            .getRemoteId(), packet.getType(), errorPacket.getLocalPacket());
            if (errorPacket.getCode() == ErrorPacket.FORWARDCODE) {
                ChannelUtils.write(localChannelReference.getNetworkChannel(), networkPacket).awaitUninterruptibly(Configuration.WAITFORWRITE);
            } else if (errorPacket.getCode() == ErrorPacket.FORWARDCLOSECODE) {
                if (!localChannelReference.getFuture().isDone()) {
                    localChannelReference.getFuture().setFailure(null);
                }
                ChannelUtils.write(localChannelReference.getNetworkChannel(), networkPacket).awaitUninterruptibly();
                logger.info("Will close channel");
                Channels.close(e.getChannel());
            }
            return;
        }
        final NetworkPacket networkPacket = new NetworkPacket(
                localChannelReference.getLocalId(), localChannelReference
                        .getRemoteId(), packet.getType(), packet.getLocalPacket());
        ChannelUtils.write(localChannelReference.getNetworkChannel(), networkPacket).awaitUninterruptibly(Configuration.WAITFORWRITE);
    }

    /*
     * (non-Javadoc)
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#exceptionCaught(org.jboss
     * .netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ExceptionEvent)
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {
        // FIXME informs network of the problem
        logger.error("Local Client Channel Exception: "
                + e.getChannel().getId(), e.getCause());
        if (localChannelReference == null) {
            initLocalClientHandler(e.getChannel());
        }
        if (localChannelReference != null) {
            OpenR66ProtocolException exception = 
                OpenR66ExceptionTrappedFactory.getExceptionFromTrappedException(e.getChannel(), e);
            if (exception != null) {
                if (exception instanceof OpenR66ProtocolShutdownException) {
                    new Thread(new ChannelUtils()).start();
                    logger.info("Will close channel");
                    Channels.close(e.getChannel());
                    return;
                }
                if (!localChannelReference.getFuture().isDone()) {
                    localChannelReference.getFuture().setFailure(exception);
                }
                final ErrorPacket errorPacket = new ErrorPacket(exception
                        .getMessage(), null, ErrorPacket.FORWARDCLOSECODE);
                final NetworkPacket networkPacket = new NetworkPacket(
                        localChannelReference.getLocalId(), localChannelReference
                                .getRemoteId(), errorPacket.getType(), errorPacket.getLocalPacket());
                ChannelUtils.write(localChannelReference.getNetworkChannel(),
                        networkPacket).awaitUninterruptibly();
            } else {
                // Nothing to do
                return;
            }
        }
        logger.info("Will close channel");
        Channels.close(e.getChannel());
    }

}
