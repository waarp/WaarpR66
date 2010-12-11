/**
   This file is part of GoldenGate Project (named also GoldenGate or GG).

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All GoldenGate Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   GoldenGate is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GoldenGate .  If not, see <http://www.gnu.org/licenses/>.
 */
package openr66.protocol.networkhandler;

import java.net.BindException;

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
import openr66.protocol.localhandler.packet.KeepAlivePacket;
import openr66.protocol.localhandler.packet.LocalPacketCodec;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
import openr66.protocol.networkhandler.packet.NetworkPacket;
import openr66.protocol.utils.ChannelUtils;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;

/**
 * Network Server Handler (Requester side)
 * @author frederic bregier
 */
public class NetworkServerHandler extends IdleStateAwareChannelHandler {
    //extends SimpleChannelHandler {
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
    /**
     * Is this Handler a server side
     */
    protected boolean isServer = false;
    /**
     * 
     * @param isServer
     */
    public NetworkServerHandler(boolean isServer) {
        this.isServer = isServer;
    }
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
        // terminate KeepAlive handler
        ReadTimeoutHandler readhandler =
            (ReadTimeoutHandler) ctx.getPipeline().get(NetworkServerPipelineFactory.READTIMEOUT);
        if (readhandler != null) {
            readhandler.releaseExternalResources();
        }
        IdleStateHandler handler =
            (IdleStateHandler) ctx.getPipeline().get(NetworkServerPipelineFactory.TIMEOUT);
        if (handler != null) {
            handler.releaseExternalResources();
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


    /* (non-Javadoc)
     * @see org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler#channelIdle(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.handler.timeout.IdleStateEvent)
     */
    @Override
    public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e)
            throws Exception {
        KeepAlivePacket keepAlivePacket = new KeepAlivePacket();
        NetworkPacket response =
            new NetworkPacket(ChannelUtils.NOCHANNEL,
                    ChannelUtils.NOCHANNEL, keepAlivePacket);
        logger.info("Write KAlive");
        Channels.write(e.getChannel(), response);
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
        } else if (packet.getCode() == LocalPacketFactory.KEEPALIVEPACKET) {
            try {
                KeepAlivePacket keepAlivePacket = (KeepAlivePacket)
                    LocalPacketCodec.decodeNetworkPacket(packet.getBuffer());
                if (keepAlivePacket.isToValidate()) {
                    keepAlivePacket.validate();
                    NetworkPacket response =
                        new NetworkPacket(ChannelUtils.NOCHANNEL,
                                ChannelUtils.NOCHANNEL, keepAlivePacket);
                    logger.info("Answer KAlive");
                    Channels.write(e.getChannel(), response);
                }
            } catch (OpenR66ProtocolPacketException e1) {
            }
            return;
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
                logger.error("Cannot create LocalChannel for: " + packet+" due to "+ e1.getMessage());
                NetworkTransaction.removeNetworkChannel(e.getChannel(), null);
                final ConnectionErrorPacket error = new ConnectionErrorPacket(
                        "Cannot connect to localChannel since cannot create it", null);
                writeError(e.getChannel(), packet.getRemoteId(), packet
                        .getLocalId(), error);
                return;
            } catch (OpenR66ProtocolRemoteShutdownException e1) {
                Configuration.configuration.getLocalTransaction()
                    .closeLocalChannelsFromNetworkChannel(e.getChannel());
                Channels.close(e.getChannel());
                //NetworkTransaction.removeForceNetworkChannel(e.getChannel());
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
        if (e.getCause() instanceof ReadTimeoutException) {
            ReadTimeoutException exception = (ReadTimeoutException) e.getCause();
            // No read for too long
            logger.error("ReadTimeout so Will close NETWORK channel {}", exception.getMessage());
            ChannelUtils.close(e.getChannel());
            return;
        }
        if (e.getCause() instanceof BindException) {
            // received when not yet connected
            logger.info("BindException");
            try {
                Thread.sleep(Configuration.WAITFORNETOP);
            } catch (InterruptedException e1) {
            }
            ChannelUtils.close(e.getChannel());
            return;
        }
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
            logger.error("Will close NETWORK channel {}", exception.getMessage());
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
    void writeError(Channel channel, Integer remoteId, Integer localId,
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
