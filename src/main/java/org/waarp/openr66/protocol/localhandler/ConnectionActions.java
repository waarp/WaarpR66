/**
   This file is part of Waarp Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All Waarp Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   Waarp is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Waarp .  If not, see <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.protocol.localhandler;

import static org.waarp.openr66.context.R66FiniteDualStates.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

import org.waarp.common.command.exception.Reply421Exception;
import org.waarp.common.command.exception.Reply530Exception;
import org.waarp.common.digest.FilesystemBasedDigest;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.openr66.commander.ClientRunner;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.context.authentication.R66Auth;
import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.Messages;
import org.waarp.openr66.protocol.exception.OpenR66Exception;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessCancelException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessNoWriteBackException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessQueryAlreadyFinishedException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessQueryStillRunningException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessRemoteFileNotFoundException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessStopException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNotAuthenticatedException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;
import org.waarp.openr66.protocol.localhandler.packet.AuthentPacket;
import org.waarp.openr66.protocol.localhandler.packet.ConnectionErrorPacket;
import org.waarp.openr66.protocol.localhandler.packet.ErrorPacket;
import org.waarp.openr66.protocol.localhandler.packet.StartupPacket;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelCloseTimer;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;

/**
 * Class to implement actions related to general connection handler: channelClosed, startup, authentication, and error.
 * Used to store and retrieve the session information.
 * 
 * @author "Frederic Bregier"
 *
 */
public abstract class ConnectionActions {
    /**
     * Internal Logger
     */
    private static final WaarpLogger logger = WaarpLoggerFactory.getLogger(ConnectionActions.class);

    /**
     * Session
     */
    protected volatile R66Session session;
    /**
     * Local Channel Reference
     */
    protected volatile LocalChannelReference localChannelReference;
    /**
     * Global Digest in receive
     */
    protected FilesystemBasedDigest globalDigest;
    /**
     * Global Digest in receive using local hash if necessary
     */
    protected FilesystemBasedDigest localDigest;

    protected void setFrom(ConnectionActions handler) {
        this.globalDigest = handler.globalDigest;
        this.localChannelReference = handler.localChannelReference;
        this.localDigest = handler.localDigest;
        this.session = handler.session;
    }

    void businessError() {
        if (session.getBusinessObject() != null) {
            session.getBusinessObject().checkAtError(session);
        }
    }
    /**
     * @return the session
     */
    public R66Session getSession() {
        return session;
    }

    /**
     * @return the localChannelReference
     */
    public LocalChannelReference getLocalChannelReference() {
        return localChannelReference;
    }

    /**
     * @return the globalDigest
     */
    public FilesystemBasedDigest getGlobalDigest() {
        return globalDigest;
    }

    /**
     * @return the localDigest
     */
    public FilesystemBasedDigest getLocalDigest() {
        return localDigest;
    }

    /**
     * Operations to ensure that channel closing is done correctly
     * 
     * @param e
     */
    public void channelClosed(ChannelHandlerContext e) {
        DbTaskRunner runner = session.getRunner();
        try {
            logger.debug("Local Server Channel Closed: {} {}",
                    (localChannelReference != null ? localChannelReference
                            : "no LocalChannelReference"), (runner != null ?
                            runner.toShortString() : "no runner"));
            // clean session objects like files
            boolean mustFinalize = true;
            if (localChannelReference != null &&
                    localChannelReference.getFutureRequest().isDone()) {
                // already done
                mustFinalize = false;
            } else {
                if (localChannelReference != null) {
                    R66Future fvr = localChannelReference.getFutureValidRequest();
                    try {
                        fvr.await(Configuration.configuration.getTIMEOUTCON() * 2, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e1) {
                    }
                    if (fvr.isDone()) {
                        if (!fvr.isSuccess()) {
                            // test if remote server was Overloaded
                            if (fvr.getResult() != null && fvr.getResult().getCode() == ErrorCode.ServerOverloaded) {
                                // ignore
                                mustFinalize = false;
                            }
                        } else {
                            mustFinalize = false;
                        }
                    }
                    logger.debug("Must Finalize: " + mustFinalize);
                    if (mustFinalize) {
                        session.newState(ERROR);
                        R66Result finalValue = new R66Result(
                                new OpenR66ProtocolSystemException(
                                        Messages.getString("LocalServerHandler.4")), //$NON-NLS-1$
                                session, true, ErrorCode.FinalOp, runner); // True since closed
                        try {
                            tryFinalizeRequest(finalValue);
                        } catch (OpenR66Exception e2) {
                        }
                    }
                }
            }
            if (mustFinalize && runner != null) {
                if (runner.isSelfRequested() && localChannelReference != null) {
                    R66Future transfer = localChannelReference.getFutureRequest();
                    // Since requested : log
                    R66Result result = transfer.getResult();
                    if (transfer.isDone() && transfer.isSuccess()) {
                        logger.info("TRANSFER REQUESTED RESULT:     SUCCESS     " +
                                (result != null ? result.toString() : "no result"));
                    } else {
                        logger.error("TRANSFER REQUESTED RESULT:     FAILURE     " +
                                (result != null ? result.toString() : "no result"));
                    }
                }
            }
            session.setStatus(50);
            session.newState(CLOSEDCHANNEL);
            session.partialClear();
            session.setStatus(51);
            if (localChannelReference != null) {
                if (localChannelReference.getDbSession() != null) {
                    localChannelReference.getDbSession().endUseConnection();
                    logger.debug("End Use Connection");
                }
                NetworkTransaction.checkClosingNetworkChannel(localChannelReference.getNetworkChannelObject(),
                        localChannelReference);
                session.setStatus(52);
            } else {
                logger.debug("Local Server Channel Closed but no LocalChannelReference: " +
                        e.channel().id());
            }
            // Now if runner is not yet finished, finish it by force
            if (mustFinalize && localChannelReference != null
                    && (!localChannelReference.getFutureRequest().isDone())) {
                R66Result finalValue = new R66Result(
                        new OpenR66ProtocolSystemException(
                                Messages.getString("LocalServerHandler.11")), //$NON-NLS-1$
                        session, true, ErrorCode.FinalOp, runner);
                localChannelReference.invalidateRequest(finalValue);
                // In case stop the attached thread if any
                ClientRunner clientRunner = localChannelReference.getClientRunner();
                if (clientRunner != null) {
                    try {
                        Thread.sleep(Configuration.RETRYINMS);
                    } catch (InterruptedException e1) {
                    }
                    clientRunner.interrupt();
                }
            }
        } finally {
            if (runner != null) {
                runner.clean();
            }
            if (localChannelReference != null) {
                localChannelReference.close();
            }
        }
    }

    /**
     * Create a new Session at startup of the channel
     */
    public void newSession() {
        session = new R66Session();
        session.setStatus(60);
        session.setBusinessObject(Configuration.configuration.getR66BusinessFactory().getBusinessInterface(session));
    }

    /**
     * Startup of the session and the local channel reference
     * 
     * @param channel
     * @param packet
     * @throws OpenR66ProtocolPacketException
     */
    public void startup(Channel channel, StartupPacket packet)
            throws OpenR66ProtocolPacketException {
        for (int i = 0; i < Configuration.RETRYNB; i++) {
            localChannelReference = Configuration.configuration
                    .getLocalTransaction().getFromId(packet.getLocalId());
            if (localChannelReference != null) {
                break;
            }
        }
        if (localChannelReference == null) {
            session.newState(ERROR);
            logger.error(Messages.getString("LocalServerHandler.1")); //$NON-NLS-1$
            if (channel.isActive()) {
                ErrorPacket error = new ErrorPacket("Cannot startup connection",
                        ErrorCode.ConnectionImpossible.getCode(), ErrorPacket.FORWARDCLOSECODE);
                channel.writeAndFlush(error).addListener(ChannelFutureListener.CLOSE);
                // Cannot do writeBack(error, true);
                session.setStatus(40);
            }
            return;
        }
        if (session.getBusinessObject() != null) {
            try {
                session.getBusinessObject().checkAtConnection(session);
            } catch (OpenR66RunnerErrorException e) {
                ErrorPacket error = new ErrorPacket("Connection refused by business logic",
                        ErrorCode.ConnectionImpossible.getCode(), ErrorPacket.FORWARDCLOSECODE);
                channel.writeAndFlush(error).addListener(ChannelFutureListener.CLOSE);
                session.setStatus(40);
                return;
            }
        }
        session.newState(STARTUP);
        localChannelReference.validateStartup(true);
        session.setLocalChannelReference(localChannelReference);
        channel.writeAndFlush(packet);
        session.setStatus(41);
    }

    /**
     * Refuse a connection
     * 
     * @param channel
     * @param packet
     * @param e1
     * @throws OpenR66ProtocolPacketException
     */
    private final void refusedConnection(Channel channel, AuthentPacket packet, Exception e1)
            throws OpenR66ProtocolPacketException {
        logger.error(Messages.getString("LocalServerHandler.6") + //$NON-NLS-1$
                localChannelReference.getNetworkChannel().remoteAddress() +
                " : " + packet.getHostId());
        logger.debug(Messages.getString("LocalServerHandler.6") + //$NON-NLS-1$
                localChannelReference.getNetworkChannel().remoteAddress() +
                " : " + packet.getHostId(), e1);
        if (Configuration.configuration.getR66Mib() != null) {
            Configuration.configuration.getR66Mib().notifyError(
                    "Connection not allowed from " +
                            localChannelReference.getNetworkChannel().remoteAddress()
                            + " since " + e1.getMessage(), packet.getHostId());
        }
        DbHostAuth auth = R66Auth.getServerAuth(localChannelReference.getDbSession(),
                packet.getHostId());
        if (auth != null && !auth.isActive()) {
            e1 = new Reply530Exception("Host is Inactive therefore connection is refused");
        }
        R66Result result = new R66Result(
                new OpenR66ProtocolSystemException(
                        Messages.getString("LocalServerHandler.6") + //$NON-NLS-1$
                                localChannelReference.getNetworkChannel().remoteAddress(),
                        e1), session, true,
                ErrorCode.BadAuthent, null);
        localChannelReference.invalidateRequest(result);
        session.newState(ERROR);
        ErrorPacket error = new ErrorPacket(e1.getMessage(),
                ErrorCode.BadAuthent.getCode(),
                ErrorPacket.FORWARDCLOSECODE);
        ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
        localChannelReference.validateConnection(false, result);
        Channel networkchannel = localChannelReference.getNetworkChannel();
        boolean valid = NetworkTransaction.shuttingDownNetworkChannelBlackList(localChannelReference
                .getNetworkChannelObject());
        logger.warn("Closing and blacklisting NetworkChannelReference since LocalChannel is not authenticated: "
                + valid);
        ChannelCloseTimer.closeFutureChannel(channel);
        ChannelCloseTimer.closeFutureChannel(networkchannel);
        businessError();
    }

    /**
     * Authentication
     * 
     * @param channel
     * @param packet
     * @throws OpenR66ProtocolPacketException
     */
    public void authent(Channel channel, AuthentPacket packet)
            throws OpenR66ProtocolPacketException {
        if (packet.isToValidate()) {
            session.newState(AUTHENTR);
        }

        if (localChannelReference.getDbSession() != null) {
            localChannelReference.getDbSession().useConnection();
        }
        if (localChannelReference.getNetworkChannelObject() != null) {
            localChannelReference.getNetworkChannelObject().setHostId(packet.getHostId());
        } else {
            session.newState(ERROR);
            logger.error("Service unavailable: " + packet.getHostId());
            R66Result result = new R66Result(
                    new OpenR66ProtocolSystemException("Service unavailable"), session, true,
                    ErrorCode.ConnectionImpossible, null);
            localChannelReference.invalidateRequest(result);
            ErrorPacket error = new ErrorPacket("Service unavailable",
                    ErrorCode.ConnectionImpossible.getCode(),
                    ErrorPacket.FORWARDCLOSECODE);
            ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
            localChannelReference.validateConnection(false, result);
            ChannelCloseTimer.closeFutureChannel(channel);
            session.setStatus(43);
            businessError();
            return;
        }
        try {
            session.getAuth().connection(localChannelReference.getDbSession(),
                    packet.getHostId(), packet.getKey());
        } catch (Reply530Exception e1) {
            refusedConnection(channel, packet, e1);
            session.setStatus(42);
            return;
        } catch (Reply421Exception e1) {
            session.newState(ERROR);
            logger.error("Service unavailable: " + packet.getHostId(), e1);
            R66Result result = new R66Result(
                    new OpenR66ProtocolSystemException("Service unavailable",
                            e1), session, true,
                    ErrorCode.ConnectionImpossible, null);
            localChannelReference.invalidateRequest(result);
            ErrorPacket error = new ErrorPacket("Service unavailable",
                    ErrorCode.ConnectionImpossible.getCode(),
                    ErrorPacket.FORWARDCLOSECODE);
            ChannelUtils.writeAbstractLocalPacket(localChannelReference, error, true);
            localChannelReference.validateConnection(false, result);
            ChannelCloseTimer.closeFutureChannel(channel);
            session.setStatus(43);
            businessError();
            return;
        }
        localChannelReference.setPartner(packet.getHostId());
        // Now if configuration say to do so: check remote ip address
        if (Configuration.configuration.isCheckRemoteAddress() && !localChannelReference.getPartner().isProxified()) {
            DbHostAuth host = R66Auth.getServerAuth(localChannelReference.getDbSession(),
                    packet.getHostId());
            boolean toTest = false;
            if (!host.isProxified()) {
                if (host.isClient()) {
                    if (Configuration.configuration.isCheckClientAddress()) {
                        if (host.isNoAddress()) {
                            // 0.0.0.0 so nothing
                            toTest = false;
                        } else {
                            toTest = true;
                        }
                    }
                } else {
                    toTest = true;
                }
            }
            if (toTest) {
                // Real address so compare
                String address = host.getAddress();
                InetAddress[] inetAddress = null;
                try {
                    inetAddress = InetAddress.getAllByName(address);
                } catch (UnknownHostException e) {
                    inetAddress = null;
                }
                if (inetAddress != null) {
                    InetSocketAddress socketAddress = (InetSocketAddress) session
                            .getRemoteAddress();
                    boolean found = false;
                    for (int i = 0; i < inetAddress.length; i++) {
                        if (socketAddress.getAddress().equals(inetAddress[i])) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        // error
                        refusedConnection(channel, packet,
                                new OpenR66ProtocolNotAuthenticatedException(
                                        "Server IP not authenticated: " +
                                                inetAddress[0].toString() + " compare to "
                                                + socketAddress.getAddress().toString()));
                        session.setStatus(104);
                        return;
                    }
                }
            }
        }
        if (session.getBusinessObject() != null) {
            try {
                session.getBusinessObject().checkAtAuthentication(session);
            } catch (OpenR66RunnerErrorException e) {
                refusedConnection(channel, packet,
                        new OpenR66ProtocolNotAuthenticatedException(e.getMessage()));
                session.setStatus(104);
                return;
            }
        }
        R66Result result = new R66Result(session, true, ErrorCode.InitOk, null);
        session.newState(AUTHENTD);
        localChannelReference.validateConnection(true, result);
        logger.debug("Local Server Channel Validated: {} ",
                (localChannelReference != null ? localChannelReference
                        : "no LocalChannelReference"));
        session.setStatus(44);
        NetworkTransaction.addClient(localChannelReference.getNetworkChannelObject(), packet.getHostId());
        if (packet.isToValidate()) {
            // only requested
            packet.validate(session.getAuth().isSsl());
            ChannelUtils.writeAbstractLocalPacket(localChannelReference, packet, false);
            session.setStatus(98);
        }
        logger.debug("Partner: {} from {}", localChannelReference.getPartner(), Configuration.configuration.getVersions());
    }

    /**
     * Receive a connection error
     * 
     * @param channel
     * @param packet
     */
    public void connectionError(Channel channel, ConnectionErrorPacket packet) {
        // do something according to the error
        logger.error(channel.id() + ": " + packet.toString());
        ErrorCode code = ErrorCode.ConnectionImpossible;
        if (packet.getSmiddle() != null) {
            code = ErrorCode.getFromCode(packet.getSmiddle());
        }
        localChannelReference.invalidateRequest(new R66Result(
                new OpenR66ProtocolSystemException(packet.getSheader()),
                session, true, code, null));
        // True since closing
        session.newState(ERROR);
        session.setStatus(45);
        channel.close();
        businessError();
    }

    /**
     * Class to finalize a runner when the future is over
     * 
     * @author Frederic Bregier
     * 
     */
    private static final class RunnerChannelFutureListener implements ChannelFutureListener {
        private LocalChannelReference localChannelReference;
        private R66Result result;

        public RunnerChannelFutureListener(LocalChannelReference localChannelReference,
                R66Result result) {
            this.localChannelReference = localChannelReference;
            this.result = result;
        }

        public void operationComplete(ChannelFuture future) throws Exception {
            localChannelReference.invalidateRequest(result);
            ChannelCloseTimer.closeFutureChannel(localChannelReference.getLocalChannel());
        }

    }

    /**
     * Receive a remote error
     * 
     * @param channel
     * @param packet
     * @throws OpenR66RunnerErrorException
     * @throws OpenR66ProtocolSystemException
     * @throws OpenR66ProtocolBusinessException
     */
    public void errorMesg(Channel channel, ErrorPacket packet)
            throws OpenR66RunnerErrorException, OpenR66ProtocolSystemException,
            OpenR66ProtocolBusinessException {
        // do something according to the error
        if (session.getLocalChannelReference().getFutureRequest().isDone()) {
            // already canceled or successful
            return;
        }
        logger.error(channel.id() + ": " + packet.toString());
        session.setStatus(46);
        ErrorCode code = ErrorCode.getFromCode(packet.getSmiddle());
        session.getLocalChannelReference().setErrorMessage(packet.getSheader(), code);
        OpenR66ProtocolBusinessException exception;
        if (code.code == ErrorCode.CanceledTransfer.code) {
            exception =
                    new OpenR66ProtocolBusinessCancelException(packet.getSheader());
            int rank = 0;
            DbTaskRunner runner = this.session.getRunner();
            if (runner != null) {
                runner.setRankAtStartup(rank);
                runner.stopOrCancelRunner(code);
            }
            R66Result result = new R66Result(exception, session,
                    true, code, runner);
            // now try to inform other
            session.setFinalizeTransfer(false, result);
            try {
                ChannelUtils.writeAbstractLocalPacket(localChannelReference, packet, false).
                        addListener(new RunnerChannelFutureListener(localChannelReference, result));
            } catch (OpenR66ProtocolPacketException e) {
            }
            return;
        } else if (code.code == ErrorCode.StoppedTransfer.code) {
            NetworkTransaction.stopRetrieve(
                    session.getLocalChannelReference());
            exception =
                    new OpenR66ProtocolBusinessStopException(packet.getSheader());
            String[] vars = packet.getSheader().split(" ");
            String var = vars[vars.length - 1];
            int rank = Integer.parseInt(var);
            DbTaskRunner runner = this.session.getRunner();
            if (runner != null) {
                if (rank < runner.getRank()) {
                    runner.setRankAtStartup(rank);
                }
                runner.stopOrCancelRunner(code);
            }
            R66Result result = new R66Result(exception, session,
                    true, code, runner);
            // now try to inform other
            session.setFinalizeTransfer(false, result);
            try {
                ChannelUtils.writeAbstractLocalPacket(localChannelReference, packet, false).
                        addListener(new RunnerChannelFutureListener(localChannelReference, result));
            } catch (OpenR66ProtocolPacketException e) {
            }
            return;
        } else if (code.code == ErrorCode.QueryAlreadyFinished.code) {
            DbTaskRunner runner = session.getRunner();
            if (runner == null) {
                exception =
                        new OpenR66ProtocolBusinessCancelException(packet.toString());
            } else {
                if (runner.isSender()) {
                    exception =
                            new OpenR66ProtocolBusinessQueryAlreadyFinishedException(
                                    packet.getSheader());
                    runner.finishTransferTask(code);
                    tryFinalizeRequest(new R66Result(exception, session, true, code, runner));
                } else {
                    exception =
                            new OpenR66ProtocolBusinessCancelException(packet.toString());
                }
            }
            throw exception;
        } else if (code.code == ErrorCode.QueryStillRunning.code) {
            exception =
                    new OpenR66ProtocolBusinessQueryStillRunningException(packet.getSheader());
            throw exception;
        } else if (code.code == ErrorCode.BadAuthent.code) {
            exception =
                    new OpenR66ProtocolNotAuthenticatedException(packet.toString());
        } else if (code.code == ErrorCode.QueryRemotelyUnknown.code) {
            exception =
                    new OpenR66ProtocolBusinessCancelException(packet.toString());
        } else if (code.code == ErrorCode.FileNotFound.code) {
            exception =
                    new OpenR66ProtocolBusinessRemoteFileNotFoundException(packet.toString());
        } else {
            exception =
                    new OpenR66ProtocolBusinessNoWriteBackException(packet.toString());
        }
        session.setFinalizeTransfer(false, new R66Result(exception, session,
                true, code, session.getRunner()));
        throw exception;
    }

    /**
     * Try to finalize the request if possible
     * 
     * @param errorValue
     *            in case of Error
     * @throws OpenR66ProtocolSystemException
     * @throws OpenR66RunnerErrorException
     */
    public final void tryFinalizeRequest(R66Result errorValue)
            throws OpenR66RunnerErrorException, OpenR66ProtocolSystemException {
        session.tryFinalizeRequest(errorValue);
    }

}
