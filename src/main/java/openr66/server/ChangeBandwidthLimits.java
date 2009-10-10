/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License, or (at your option)
 * any later version.
 *
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package openr66.server;

import java.net.SocketAddress;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;


import openr66.configuration.FileBasedConfiguration;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.database.DbConstant;
import openr66.database.data.DbHostAuth;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
import openr66.protocol.localhandler.packet.ValidPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.ChannelUtils;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.channel.Channels;
import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * This command enables the dynamic change of bandwidth limitation.
 * It does not changed the valuesin the database but only dynamic values while the
 * server is running and until it is shutdown.
 *
 * @author Frederic Bregier
 *
 */
public class ChangeBandwidthLimits implements Runnable {
    /**
     * Internal Logger
     */
    static volatile GgInternalLogger logger;

    protected final R66Future future;
    protected final long writeGlobalLimit;
    protected final long readGlobalLimit;
    protected final long writeSessionLimit;
    protected final long readSessionLimit;
    protected final NetworkTransaction networkTransaction;

    public ChangeBandwidthLimits(R66Future future, long wgl, long rgl, long wsl, long rsl,
            NetworkTransaction networkTransaction) {
        this.future = future;
        this.writeGlobalLimit = wgl;
        this.readGlobalLimit = rgl;
        this.writeSessionLimit = wsl;
        this.readSessionLimit = rsl;
        this.networkTransaction = networkTransaction;
    }

    /**
     * Prior to call this method, the pipeline and NetworkTransaction must have been initialized.
     * It is the responsibility of the caller to finish all network resources.
     */
    public void run() {
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(ChangeBandwidthLimits.class);
        }
        ValidPacket valid = new ValidPacket(writeGlobalLimit+" "+readGlobalLimit,
                writeSessionLimit+" "+readSessionLimit, LocalPacketFactory.BANDWIDTHPACKET);
        DbHostAuth host = Configuration.configuration.HOST_AUTH;
        SocketAddress socketAddress = host.getSocketAddress();
        boolean isSSL = host.isSsl();
        LocalChannelReference localChannelReference = networkTransaction
            .createConnectionWithRetry(socketAddress, isSSL, future);
        socketAddress = null;
        if (localChannelReference == null) {
            host = null;
            logger.error("Cannot Connect");
            future.setResult(new R66Result(
                    new OpenR66ProtocolNoConnectionException("Cannot connect to server"),
                    null, true, ErrorCode.Internal));
            future.setFailure(future.getResult().exception);
            return;
        }
        try {
            ChannelUtils.writeAbstractLocalPacket(localChannelReference, valid);
        } catch (OpenR66ProtocolPacketException e) {
            logger.error("Bad Protocol", e);
            Channels.close(localChannelReference.getLocalChannel());
            localChannelReference = null;
            host = null;
            valid = null;
            future.setResult(new R66Result(e, null, true,
                    ErrorCode.TransferError));
            future.setFailure(e);
            return;
        }
        host = null;
        future.awaitUninterruptibly();
        logger.info("Request done with "+(future.isSuccess()?"success":"error"));
        Channels.close(localChannelReference.getLocalChannel());
        localChannelReference = null;
    }

    protected static long swriteGlobalLimit = -1;
    protected static long sreadGlobalLimit = -1;
    protected static long swriteSessionLimit = -1;
    protected static long sreadSessionLimit = -1;

    protected static boolean getParams(String [] args) {
        if (args.length < 1) {
            logger.error("Need at least the configuration file as first argument");
            return false;
        }
        if (! FileBasedConfiguration
                .setClientConfigurationFromXml(args[0])) {
            logger
                    .error("Needs a correct configuration file as first argument");
            return false;
        }
        for (int i = 1; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-wglob")) {
                i++;
                swriteGlobalLimit = Long.parseLong(args[i]);
            } else if (args[i].equalsIgnoreCase("-rglob")) {
                i++;
                sreadGlobalLimit = Long.parseLong(args[i]);
            } else if (args[i].equalsIgnoreCase("-wsess")) {
                i++;
                swriteSessionLimit = Long.parseLong(args[i]);
            } else if (args[i].equalsIgnoreCase("-rsess")) {
                i++;
                sreadSessionLimit = Long.parseLong(args[i]);
            }
        }
        return true;
    }

    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(ChangeBandwidthLimits.class);
        }
        if (! getParams(args)) {
            logger.error("Wrong initialization");
            if (DbConstant.admin != null && DbConstant.admin.isConnected) {
                DbConstant.admin.close();
            }
            System.exit(1);
        }
        long time1 = System.currentTimeMillis();
        R66Future future = new R66Future(true);

        Configuration.configuration.pipelineInit();
        NetworkTransaction networkTransaction = new NetworkTransaction();
        try {
            ChangeBandwidthLimits transaction = new ChangeBandwidthLimits(future,
                    swriteGlobalLimit, sreadGlobalLimit, swriteSessionLimit, sreadSessionLimit,
                    networkTransaction);
            transaction.run();
            future.awaitUninterruptibly();
            long time2 = System.currentTimeMillis();
            long delay = time2 - time1;
            R66Result result = future.getResult();
            if (future.isSuccess()) {
                if (result.code == ErrorCode.Warning) {
                    logger.warn("Warning on bandwidth: " +
                            (result.other != null? ((ValidPacket)result.other).getSheader() :
                                "no file")
                            +" delay: "+delay);
                } else {
                    logger.warn("Success on Bandwidth: " +
                            (result.other != null? ((ValidPacket)result.other).getSheader() :
                            "no file")
                            +" delay: "+delay);
                }
            } else {
                if (result.code == ErrorCode.Warning) {
                    logger.warn("Bandwidth in Warning", future.getCause());
                    networkTransaction.closeAll();
                    System.exit(result.code.ordinal());
                } else {
                    logger.error("Bandwidth in Error", future.getCause());
                    networkTransaction.closeAll();
                    System.exit(result.code.ordinal());
                }
            }
        } finally {
            networkTransaction.closeAll();
        }
    }

}
