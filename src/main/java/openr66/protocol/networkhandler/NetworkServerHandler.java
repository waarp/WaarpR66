/**
 *
 */
package openr66.protocol.networkhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.database.DbConstant;
import openr66.database.DbSession;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66Exception;
import openr66.protocol.exception.OpenR66ExceptionTrappedFactory;
import openr66.protocol.exception.OpenR66ProtocolBusinessNoWriteBackException;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.AbstractLocalPacket;
import openr66.protocol.localhandler.packet.ConnectionErrorPacket;
import openr66.protocol.localhandler.packet.LocalPacketCodec;
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
 * Network Server Handler (Requester side)
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
    /**
     * The underlying Network Channel
     */
    private volatile Channel networkChannel;
    /**
     * The Database connection attached to this NetworkChannel
     * shared among all associated LocalChannels
     */
    protected volatile DbSession dbSession;
    /**
     * Does this Handler is for SSL
     */
    protected volatile boolean isSSL = false;
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
            logger.info("Network Channel Closed: {} LocalChannels Left: {}",
                    e.getChannel().getId(),
                    NetworkTransaction.getNbLocalChannel(e.getChannel()));
            // close if necessary the local channel
            Configuration.configuration.getLocalTransaction()
                    .closeLocalChannelsFromNetworkChannel(e.getChannel());
        }
        NetworkTransaction.removeForceNetworkChannel(e.getChannel());
        //Now force the close of the database after a wait
        if (dbSession != null && dbSession.internalId != DbConstant.admin.session.internalId) {
            dbSession.disconnect();
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
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws OpenR66ProtocolNetworkException {
        this.networkChannel = e.getChannel();
        try {
            if (DbConstant.admin.isConnected) {
                this.dbSession = new DbSession(DbConstant.admin, false);
            }
        } catch (OpenR66DatabaseNoConnectionError e1) {
            // Cannot connect so use default connection
            logger.warn("Use default database connection");
            this.dbSession = DbConstant.admin.session;
        }
        logger.info("Network Channel Connected: {} ", e.getChannel().getId());
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
        if (packet.getCode() == LocalPacketFactory.CONNECTERRORPACKET) {
            logger.info("NetworkRecv: {}",packet);
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
            logger.info("NetworkRecv Create: {} {}",packet,
                    e.getChannel().getId());
            try {
                localChannelReference =
                    NetworkTransaction.createConnectionFromNetworkChannelStartup(
                            e.getChannel(), packet);
            } catch (OpenR66ProtocolSystemException e1) {
                logger.warn("Cannot create LocalChannel for: " + packet+" due to "+ e1.getMessage());
                NetworkTransaction.removeNetworkChannel(e.getChannel(), null);
                final ConnectionErrorPacket error = new ConnectionErrorPacket(
                        "Cannot connect to localChannel since cannot create it", null);
                writeError(e.getChannel(), packet.getRemoteId(), packet
                        .getLocalId(), error);
                return;
            } catch (OpenR66ProtocolRemoteShutdownException e1) {
                Configuration.configuration.getLocalTransaction()
                    .closeLocalChannelsFromNetworkChannel(e.getChannel());
                NetworkTransaction.removeForceNetworkChannel(e.getChannel());
                // ignore since no more valid
                return;
            }
        } else {
            if (packet.getCode() == LocalPacketFactory.ENDREQUESTPACKET) {
                // Not a local error but a remote one
                try {
                    localChannelReference = Configuration.configuration
                            .getLocalTransaction().getClient(packet.getRemoteId(),
                                    packet.getLocalId());
                } catch (OpenR66ProtocolSystemException e1) {
                    // do not send anything since the packet is external
                    try {
                        logger.info("Cannot get LocalChannel while an end of request comes: {}",
                                LocalPacketCodec.decodeNetworkPacket(packet.getBuffer()));
                    } catch (OpenR66ProtocolPacketException e2) {
                        logger.info("Cannot get LocalChannel while an end of request comes: {}",
                                packet.toString());
                    }
                    return;
                }
                // OK continue and send to the local channel
            } else if (packet.getCode() == LocalPacketFactory.CONNECTERRORPACKET) {
                // Not a local error but a remote one
                try {
                    localChannelReference = Configuration.configuration
                            .getLocalTransaction().getClient(packet.getRemoteId(),
                                    packet.getLocalId());
                } catch (OpenR66ProtocolSystemException e1) {
                    // do not send anything since the packet is external
                    try {
                        logger.info("Cannot get LocalChannel while an external error comes: {}",
                                LocalPacketCodec.decodeNetworkPacket(packet.getBuffer()));
                    } catch (OpenR66ProtocolPacketException e2) {
                        logger.info("Cannot get LocalChannel while an external error comes: {}",
                                packet.toString());
                    }
                    return;
                }
                // OK continue and send to the local channel
            } else {
                try {
                    localChannelReference = Configuration.configuration
                            .getLocalTransaction().getClient(packet.getRemoteId(),
                                    packet.getLocalId());
                } catch (OpenR66ProtocolSystemException e1) {
                    if (NetworkTransaction.isShuttingdownNetworkChannel(e
                            .getChannel())) {
                        // ignore
                        return;
                    }
                    logger.info("Cannot get LocalChannel: " + packet + " due to " +
                            e1.getMessage());
                    final ConnectionErrorPacket error = new ConnectionErrorPacket(
                            "Cannot get localChannel since cannot retrieve it", null);
                    writeError(e.getChannel(), packet.getRemoteId(), packet
                            .getLocalId(), error);
                    return;
                }
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
        logger.info("Network Channel Exception: {}",e.getChannel().getId(), e
                .getCause());
        OpenR66Exception exception = OpenR66ExceptionTrappedFactory
                .getExceptionFromTrappedException(e.getChannel(), e);
        if (exception != null) {
            if (exception instanceof OpenR66ProtocolBusinessNoWriteBackException) {
                if (NetworkTransaction.getNbLocalChannel(e.getChannel()) > 0) {
                    logger.info(
                            "Network Channel Exception: {} {}", e.getChannel().getId(),
                            exception.getMessage());
                }
                logger.info("Will close NETWORK channel");
                try {
                    Thread.sleep(Configuration.WAITFORNETOP);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
                ChannelUtils.close(e.getChannel());
                return;
            } else if (exception instanceof OpenR66ProtocolNoConnectionException) {
                logger.info("Connection impossible with NETWORK channel {}",
                        exception.getMessage());
                Channels.close(e.getChannel());
                return;
            } else {
                logger.error(
                        "Network Channel Exception: {} {}", e.getChannel().getId(),
                        exception.getMessage());
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
     *
     * @return True if the networkChannel is writable
     */
    public boolean isWritable() {
        if (!networkChannel.isWritable()) {
            isWriteReady = false;
        }
        return isWriteReady;
    }
    /**
     * Channel is reday
     * @return True if the networkChannel is writable again
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

    /**
     * @return the dbSession
     */
    public DbSession getDbSession() {
        return dbSession;
    }
    /**
     *
     * @return True if this Handler is for SSL
     */
    public boolean isSsl() {
        return isSSL;
    }
}
