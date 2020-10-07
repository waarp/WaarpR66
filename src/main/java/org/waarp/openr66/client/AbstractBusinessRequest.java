/**
 * This file is part of Waarp Project.
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All Waarp Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * 
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with Waarp . If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.client;

import java.net.SocketAddress;

import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.common.utility.DetectionUtils;
import org.waarp.openr66.client.utils.OutputFormat;
import org.waarp.openr66.configuration.FileBasedConfiguration;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66FiniteDualStates;
import org.waarp.openr66.context.authentication.R66Auth;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.Messages;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;
import org.waarp.openr66.protocol.localhandler.packet.BusinessRequestPacket;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;

/**
 * Abstract class for internal Business Request
 * 
 * @author Frederic Bregier
 * 
 */
public abstract class AbstractBusinessRequest implements Runnable {
    /**
     * Internal Logger
     */
    static protected volatile WaarpLogger logger;

    protected static String _INFO_ARGS = Messages.getString("AbstractBusinessRequest.0") + Messages.getString("Message.OutputFormat"); //$NON-NLS-1$

    protected final R66Future future;

    protected final String remoteHost;

    protected final NetworkTransaction networkTransaction;

    private final BusinessRequestPacket businessPacket;

    private LocalChannelReference localChannelReference;

    public AbstractBusinessRequest(Class<?> clasz,
            R66Future future,
            String remoteHost,
            NetworkTransaction networkTransaction,
            BusinessRequestPacket packet) {
        if (logger == null) {
            logger = WaarpLoggerFactory.getLogger(clasz);
        }
        this.future = future;
        this.remoteHost = remoteHost;
        this.networkTransaction = networkTransaction;
        this.businessPacket = packet;
    }

    public void run() {
        try {
            initRequest();
            sendRequest();
        } catch (OpenR66ProtocolNoConnectionException e) {
        }
    }

    public void initRequest() throws OpenR66ProtocolNoConnectionException {
        DbHostAuth host = R66Auth.getServerAuth(DbConstant.admin.getSession(),
                remoteHost);
        if (host == null) {
            future.setResult(null);
            OpenR66ProtocolNoConnectionException e2 =
                    new OpenR66ProtocolNoConnectionException(
                            Messages.getString("AdminR66OperationsGui.188") + remoteHost); //$NON-NLS-1$
            future.setFailure(e2);
            throw e2;
        }
        final SocketAddress socketServerAddress;
        try {
            socketServerAddress = host.getSocketAddress();
        } catch (IllegalArgumentException e) {
            future.setResult(null);
            OpenR66ProtocolNoConnectionException e2 =
                    new OpenR66ProtocolNoConnectionException(
                            Messages.getString("AdminR66OperationsGui.188") + host.toString()); //$NON-NLS-1$
            future.setFailure(e2);
            throw e2;
        }
        boolean isSSL = host.isSsl();
        localChannelReference = networkTransaction
                .createConnectionWithRetry(socketServerAddress, isSSL, future);
        if (localChannelReference == null) {
            future.setResult(null);
            OpenR66ProtocolNoConnectionException e =
                    new OpenR66ProtocolNoConnectionException(
                            Messages.getString("AdminR66OperationsGui.188") + host.toString()); //$NON-NLS-1$
            future.setFailure(e);
            throw e;
        }
        localChannelReference.sessionNewState(R66FiniteDualStates.BUSINESSR);
    }

    public void sendRequest() {
        try {
            ChannelUtils.writeAbstractLocalPacket(localChannelReference, businessPacket, false);
        } catch (OpenR66ProtocolPacketException e) {
            future.setResult(null);
            future.setFailure(e);
            localChannelReference.getLocalChannel().close();
            return;
        }

    }

    /**
     * Dummy Main method
     * 
     * @param args
     */
    public static void main(String[] args) {
        WaarpLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(
                null));
        if (logger == null) {
            logger = WaarpLoggerFactory.getLogger(AbstractBusinessRequest.class);
        }
        if (!getParams(args)) {
            logger.error("Wrong initialization");
            if (DbConstant.admin != null && DbConstant.admin.isActive()) {
                DbConstant.admin.close();
            }
            if (DetectionUtils.isJunit()) {
                return;
            }
            ChannelUtils.stopLogger();
            System.exit(2);
        }

        Configuration.configuration.pipelineInit();
        NetworkTransaction networkTransaction = new NetworkTransaction();
        R66Future future = new R66Future(true);

        logger.info("Start Test of Transaction");
        long time1 = System.currentTimeMillis();

        @SuppressWarnings("unused")
        BusinessRequestPacket packet =
                new BusinessRequestPacket(classname + " " + classarg, 0);
        // XXX FIXME this has to be adapted
        /*
         * AbstractBusinessRequest transaction = new AbstractBusinessRequest(
         * AbstractBusinessRequest.class, future, rhost, networkTransaction, packet);
         * transaction.run(); future.awaitUninterruptibly();
         */
        long time2 = System.currentTimeMillis();
        logger.debug("Finish Business Request: " + future.isSuccess());
        long delay = time2 - time1;
        if (future.isSuccess()) {
            logger.info("Business Request in status: SUCCESS" +
                    "    <REMOTE>" + rhost + "</REMOTE>" +
                    "    delay: " + delay);
        } else {
            logger.info("Business Request in status: FAILURE" +
                    "    <REMOTE>" + rhost + "</REMOTE>" +
                    "    <ERROR>" + future.getCause() + "</ERROR>" +
                    "    delay: " + delay);
            if (DetectionUtils.isJunit()) {
                return;
            }
            networkTransaction.closeAll();
            System.exit(ErrorCode.Unknown.ordinal());
        }
        networkTransaction.closeAll();
    }

    static protected String rhost = null;
    static protected String classname = null;
    static protected String classarg = null;
    static protected boolean nolog = false;

    /**
     * Parse the parameter and set current values
     * 
     * @param args
     * @return True if all parameters were found and correct
     */
    protected static boolean getParams(String[] args) {
        _INFO_ARGS = Messages.getString("AbstractBusinessRequest.0") + Messages.getString("Message.OutputFormat"); //$NON-NLS-1$
        if (args.length < 3) {
            logger
                    .error(_INFO_ARGS);
            return false;
        }
        if (!FileBasedConfiguration
                .setClientConfigurationFromXml(Configuration.configuration, args[0])) {
            logger
                    .error(Messages.getString("Configuration.NeedCorrectConfig")); //$NON-NLS-1$
            return false;
        }
        // Now set default values from configuration
        for (int i = 1; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-to")) {
                i++;
                rhost = args[i];
                if (Configuration.configuration.getAliases().containsKey(rhost)) {
                    rhost = Configuration.configuration.getAliases().get(rhost);
                }
            } else if (args[i].equalsIgnoreCase("-class")) {
                i++;
                classname = args[i];
            } else if (args[i].equalsIgnoreCase("-arg")) {
                i++;
                classarg = args[i];
            } else if (args[i].equalsIgnoreCase("-nolog")) {
                nolog = true;
                i++;
            }
        }
        OutputFormat.getParams(args);
        if (rhost != null && classname != null) {
            return true;
        }
        logger.error(Messages.getString("AbstractBusinessRequest.NeedMoreArgs", "(-to -class)") + _INFO_ARGS); //$NON-NLS-1$
        return false;
    }

}
