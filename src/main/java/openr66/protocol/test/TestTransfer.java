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
package openr66.protocol.test;

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import openr66.context.filesystem.R66File;
import openr66.database.DbConstant;
import openr66.protocol.config.Configuration;
import openr66.protocol.config.R66FileBasedConfiguration;
import openr66.protocol.exception.OpenR66Exception;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.networkhandler.packet.NetworkPacket;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.channel.Channels;
import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * @author Frederic Bregier
 *
 */
public class TestTransfer implements Runnable {
    /**
     * Internal Logger
     */
    private static GgInternalLogger logger;

    final private NetworkTransaction networkTransaction;

    final private R66Future future;

    private final SocketAddress socketAddress;

    final private String filename;

    final private String rulename;

    public TestTransfer(NetworkTransaction networkTransaction,
            R66Future future, SocketAddress socketAddress, String filename,
            String rulename) {
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(TestTransfer.class);
        }
        this.networkTransaction = networkTransaction;
        this.future = future;
        this.socketAddress = socketAddress;
        this.filename = filename;
        this.rulename = rulename;
    }

    public void run() {
        LocalChannelReference localChannelReference = null;
        OpenR66Exception lastException = null;
        for (int i = 0; i < Configuration.RETRYNB; i ++) {
            try {
                localChannelReference = networkTransaction
                        .createConnection(socketAddress);
                break;
            } catch (OpenR66ProtocolNetworkException e1) {
                lastException = e1;
                localChannelReference = null;
            } catch (OpenR66ProtocolRemoteShutdownException e1) {
                lastException = e1;
                localChannelReference = null;
                break;
            } catch (OpenR66ProtocolNoConnectionException e1) {
                lastException = e1;
                localChannelReference = null;
                break;
            }
        }
        if (localChannelReference == null) {
            logger.error("Cannot connect", lastException);
            future.setResult(null);
            future.setFailure(lastException);
            return;
        } else if (lastException != null) {
            logger.warn("Connection retry since ", lastException);
        }
        // FIXME data transfer
        int block = 101;
        //int block = Configuration.configuration.BLOCKSIZE;
        RequestPacket request = new RequestPacket(rulename,
                RequestPacket.RECVMODE, filename, block, 0, DbConstant.ILLEGALVALUE,
                "MONTEST test.xml");
        NetworkPacket networkPacket;
        try {
            networkPacket = new NetworkPacket(localChannelReference
                    .getLocalId(), localChannelReference.getRemoteId(), request);
        } catch (OpenR66ProtocolPacketException e) {
            future.setResult(null);
            future.setFailure(e);
            Channels.close(localChannelReference.getLocalChannel());
            return;
        }
        Channels
                .write(localChannelReference.getNetworkChannel(), networkPacket);
        // FIXME ne pas fermer la connection locale ni distante, instancier la
        // connection locale
        // avec la request, enchainer sur validation de request sur la
        // transmission
        localChannelReference.getFutureAction().awaitUninterruptibly();
        if (localChannelReference.getFutureAction().isSuccess()) {
            future.setResult(localChannelReference.getFutureAction()
                    .getResult());
            future.setSuccess();
        } else {
            future.setResult(null);
            Throwable throwable = localChannelReference.getFutureAction()
                    .getCause();
            if (throwable == null) {
                future.cancel();
            } else {
                future.setFailure(throwable);
            }
        }
    }

    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(TestTransfer.class);
        }
        if (args.length < 3) {
            logger
                    .error("Needs at least the configuration file, the file to transfer, the rule as arguments");
            return;
        }
        Configuration.configuration.fileBasedConfiguration = new R66FileBasedConfiguration();
        if (!Configuration.configuration.fileBasedConfiguration
                .setConfigurationFromXml(args[0])) {
            logger
                    .error("Needs a correct configuration file as first argument");
            return;
        }
        String localFilename = args[1];
        String rule = args[2];
        Configuration.configuration.pipelineInit();

        final NetworkTransaction networkTransaction = new NetworkTransaction();
        final SocketAddress socketServerAddress = new InetSocketAddress(
                Configuration.configuration.SERVER_PORT);

        ExecutorService executorService = Executors.newCachedThreadPool();
        int nb = 10;

        R66Future[] arrayFuture = new R66Future[nb];
        logger.warn("Start");
        long time1 = System.currentTimeMillis();
        for (int i = 0; i < nb; i ++) {
            arrayFuture[i] = new R66Future(true);
            TestTransfer transaction = new TestTransfer(networkTransaction,
                    arrayFuture[i], socketServerAddress, localFilename, rule);
            executorService.execute(transaction);
        }
        int success = 0;
        int error = 0;
        for (int i = 0; i < nb; i ++) {
            arrayFuture[i].awaitUninterruptibly();
            if (arrayFuture[i].isSuccess()) {
                success ++;
            } else {
                error ++;
            }
        }
        long time2 = System.currentTimeMillis();
        long length = 0;
        logger
                .warn("Final file: " +
                        (arrayFuture[0].getResult() instanceof R66File? ((R66File) arrayFuture[0]
                                .getResult()).toString()
                                : "no file"));
        try {
            length = arrayFuture[0].getResult() instanceof R66File? ((R66File) arrayFuture[0]
                    .getResult()).length()
                    : 0L;
        } catch (CommandAbstractException e) {
        }
        long delay = time2 - time1;
        float nbs = success * 1000;
        nbs /= delay;
        float mbs = nbs * length / 1024;
        logger.error("Success: " + success + " Error: " + error + " delay: " +
                delay + " NB/s: " + nbs + " KB/s: " + mbs);
        executorService.shutdown();
        networkTransaction.closeAll();
    }

}
