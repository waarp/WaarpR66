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
package openr66.server;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;

import java.net.SocketAddress;

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

/**
 * Config Export from a local client without database connection
 *
 * @author Frederic Bregier
 *
 */
public class ConfigExport implements Runnable {
    /**
     * Internal Logger
     */
    static volatile GgInternalLogger logger;

    protected final R66Future future;
    protected final boolean host;
    protected final boolean rule;
    protected final NetworkTransaction networkTransaction;

    public ConfigExport(R66Future future, boolean host, boolean rule,
            NetworkTransaction networkTransaction) {
        this.future = future;
        this.host = host;
        this.rule = rule;
        this.networkTransaction = networkTransaction;
    }
    /**
     * Prior to call this method, the pipeline and NetworkTransaction must have been initialized.
     * It is the responsibility of the caller to finish all network resources.
     */
    public void run() {
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(ConfigExport.class);
        }
        ValidPacket valid = new ValidPacket(Boolean.toString(host), Boolean.toString(rule), 
                LocalPacketFactory.CONFEXPORTPACKET);
        DbHostAuth host = Configuration.configuration.HOST_SSLAUTH;
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
                    null, true, ErrorCode.Internal, null));
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
                    ErrorCode.TransferError, null));
            future.setFailure(e);
            return;
        }
        host = null;
        future.awaitUninterruptibly();
        logger.info("Request done with "+(future.isSuccess()?"success":"error"));
        Channels.close(localChannelReference.getLocalChannel());
        localChannelReference = null;
    }

    protected static boolean shost = false;
    protected static boolean srule = false;

    protected static boolean getParams(String [] args) {
        if (args.length < 2) {
            logger.error("Need at least the configuration file as first argument then at least one from\n" +
            		"    -hosts\n" +
            		"    -rules");
            return false;
        }
        if (! FileBasedConfiguration
                .setClientConfigurationFromXml(args[0])) {
            logger.error("Need at least the configuration file as first argument then at least one from\n" +
                        "    -hosts\n" +
                        "    -rules");
            return false;
        }
        for (int i = 1; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-hosts")) {
                shost = true;
            } else if (args[i].equalsIgnoreCase("-rules")) {
                srule = true;
            }
        }
        if ((!shost) && (!srule)) {
            logger.error("Need at least one of -hosts - rules");
            return false;
        }
        return true;
    }

    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(null));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(ConfigExport.class);
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
            ConfigExport transaction = new ConfigExport(future,
                    shost, srule,
                    networkTransaction);
            transaction.run();
            future.awaitUninterruptibly();
            long time2 = System.currentTimeMillis();
            long delay = time2 - time1;
            R66Result result = future.getResult();
            if (future.isSuccess()) {
                if (result.code == ErrorCode.Warning) {
                    logger.warn("WARNED on files:\n    " +
                            (result.other != null? ((ValidPacket)result.other).getSheader() :
                                "no file")
                            +"\n    delay: "+delay);
                } else {
                    logger.info("SUCCESS on Final files:\n    " +
                            (result.other != null? ((ValidPacket)result.other).getSheader() :
                            "no file")
                            +"\n    delay: "+delay);
                }
            } else {
                if (result.code == ErrorCode.Warning) {
                    logger.warn("Transfer is\n    WARNED", future.getCause());
                    networkTransaction.closeAll();
                    System.exit(result.code.ordinal());
                } else {
                    logger.error("Transfer in\n    FAILURE", future.getCause());
                    networkTransaction.closeAll();
                    System.exit(result.code.ordinal());
                }
            }
        } finally {
            networkTransaction.closeAll();
        }
    }

}
