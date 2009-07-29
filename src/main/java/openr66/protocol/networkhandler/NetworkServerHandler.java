/**
 *
 */
package openr66.protocol.networkhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66Exception;
import openr66.protocol.exception.OpenR66ExceptionTrappedFactory;
import openr66.protocol.exception.OpenR66ProtocolBusinessNoWriteBackException;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.AbstractLocalPacket;
import openr66.protocol.localhandler.packet.ConnectionErrorPacket;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
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
 * Network Server Handler
 * @author frederic bregier
 */
@ChannelPipelineCoverage("one")
public class NetworkServerHandler extends SimpleChannelHandler {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(NetworkServerHandler.class);
    /**
     * Used by retriever to be able to prevent OOME
     */
    private volatile boolean isWriteReady = true;

    /*
     * (non-Javadoc)
     *
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#channelClosed(org.jboss.
     * netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
        if (NetworkTransaction.getNbLocalChannel(e.getChannel()) > 0) {
            logger.warn("Network Channel Closed: " + e.getChannel().getId() +
                    " LocalChannels Left: " +
                    NetworkTransaction.getNbLocalChannel(e.getChannel()));
            // close if necessary the local channel
            Configuration.configuration.getLocalTransaction()
                    .closeLocalChannelsFromNetworkChannel(e.getChannel());
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
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        logger.info("Network Channel Connected: " + e.getChannel().getId());
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
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        final NetworkPacket packet = (NetworkPacket) e.getMessage();
        logger.info("Network Channel Recv: " + e.getChannel().getId() + " " +
                packet.toString());
        if (packet.getCode() == LocalPacketFactory.CONNECTERRORPACKET) {
            // Special code to STOP here
            if (packet.getLocalId() == ChannelUtils.NOCHANNEL) {
                // No way to know what is wrong: close all connections with
                // remote host
                logger
                        .error("Will close NETWORK channel, Cannot continue connection with remote Host: " +
                                packet.toString() +
                                " : " +
                                e.getChannel().getRemoteAddress());
                Channels.close(e.getChannel());
                return;
            }
        }
        LocalChannelReference localChannelReference = null;
        if (packet.getLocalId() == ChannelUtils.NOCHANNEL) {
            try {
                localChannelReference = Configuration.configuration
                        .getLocalTransaction().createNewClient(e.getChannel(),
                                packet.getRemoteId());
                NetworkTransaction.addNetworkChannel(e.getChannel());
                logger.info("Create LocalChannel: " +
                        localChannelReference.getLocalId());
            } catch (OpenR66ProtocolSystemException e1) {
                logger.error("Cannot create LocalChannel: " + packet, e1);
                final ConnectionErrorPacket error = new ConnectionErrorPacket(
                        "Cannot connect to localChannel", null);
                writeError(e.getChannel(), packet.getRemoteId(), packet
                        .getLocalId(), error);
                return;
            } catch (OpenR66ProtocolRemoteShutdownException e1) {
                // ignore since no more valid
                return;
            }
        } else {
            try {
                localChannelReference = Configuration.configuration
                        .getLocalTransaction().getClient(packet.getRemoteId(),
                                packet.getLocalId());
                logger.info("Get LocalChannel: " +
                        localChannelReference.getLocalId());
            } catch (OpenR66ProtocolSystemException e1) {
                if (NetworkTransaction.isShuttingdownNetworkChannel(e
                        .getChannel())) {
                    // ignore
                    return;
                }
                logger.error("Cannot get LocalChannel: " + packet + " due to " +
                        e1.getMessage());
                final ConnectionErrorPacket error = new ConnectionErrorPacket(
                        "Cannot get localChannel", null);
                writeError(e.getChannel(), packet.getRemoteId(), packet
                        .getLocalId(), error);
                return;
            }
        }
        Channels.write(localChannelReference.getLocalChannel(), packet
                .getBuffer());
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
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        logger.info("Network Channel Exception: " + e.getChannel().getId(), e
                .getCause());
        OpenR66Exception exception = OpenR66ExceptionTrappedFactory
                .getExceptionFromTrappedException(e.getChannel(), e);
        if (exception != null) {
            logger.error(
                    "Network Channel Exception: " + e.getChannel().getId(),
                    exception);
            if (exception instanceof OpenR66ProtocolBusinessNoWriteBackException) {
                logger.error("Will close NETWORK channel", exception);
                ChannelUtils.close(e.getChannel());
                return;
            } else if (exception instanceof OpenR66ProtocolNoConnectionException) {
                logger.error("Connection impossible with NETWORK channel",
                        exception);
                Channels.close(e.getChannel());
                return;
            }
            final ConnectionErrorPacket errorPacket = new ConnectionErrorPacket(
                    exception.getMessage(), null);
            writeError(e.getChannel(), ChannelUtils.NOCHANNEL,
                    ChannelUtils.NOCHANNEL, errorPacket);
            logger.error("Will close NETWORK channel", exception);
            ChannelUtils.close(e.getChannel());
        } else {
            // Nothing to do
            return;
        }
    }

    /**
     * To enable continues of Retrieve operation (prevent OOM)
     *
     * @see org.jboss.netty.channel.SimpleChannelHandler#channelInterestChanged(org.jboss.netty.channel.ChannelHandlerContext,
     *      org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelInterestChanged(ChannelHandlerContext arg0,
            ChannelStateEvent arg1) {
        int op = arg1.getChannel().getInterestOps();
        if (op == Channel.OP_NONE || op == Channel.OP_READ) {
            isWriteReady = true;
        }
    }
    /**
     * Channel is not ready
     */
    public void setWriteNotReady() {
        isWriteReady = false;
    }
    /**
     * Channel is reday
     * @return
     */
    public boolean isWriteReady() {
        return isWriteReady;

    }
    /**
     * Write error back to remote client
     * @param channel
     * @param remoteId
     * @param localId
     * @param error
     */
    private void writeError(Channel channel, Integer remoteId, Integer localId,
            AbstractLocalPacket error) {
        NetworkPacket networkPacket = null;
        try {
            networkPacket = new NetworkPacket(localId, remoteId, error);
        } catch (OpenR66ProtocolPacketException e) {
        }
        Channels.write(channel, networkPacket).awaitUninterruptibly();
    }
}
