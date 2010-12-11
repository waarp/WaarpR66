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
package openr66.protocol.test;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import openr66.configuration.FileBasedConfiguration;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66Exception;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.TestPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.ChannelUtils;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.channel.Channels;
import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * Test class for internal ping pong test
 * @author Frederic Bregier
 *
 */
public class TestTransaction implements Runnable {
    /**
     * Internal Logger
     */
    private static GgInternalLogger logger;

    final private NetworkTransaction networkTransaction;

    final private R66Future future;

    private final SocketAddress socketAddress;

    final private TestPacket testPacket;

    public TestTransaction(NetworkTransaction networkTransaction,
            R66Future future, SocketAddress socketAddress, TestPacket packet) {
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(TestTransaction.class);
        }
        this.networkTransaction = networkTransaction;
        this.future = future;
        this.socketAddress = socketAddress;
        testPacket = packet;
    }

    public void run() {
        LocalChannelReference localChannelReference = null;
        OpenR66Exception lastException = null;
        for (int i = 0; i < Configuration.RETRYNB; i ++) {
            try {
                localChannelReference = networkTransaction
                        .createConnection(socketAddress, false, future);
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
            logger.error("Cannot connect: " + lastException.getMessage());
            future.setResult(null);
            if (lastException == null) {
                future.cancel();
            } else {
                future.setFailure(lastException);
            }
            return;
        } else if (lastException != null) {
            logger.warn("Connection retry since ", lastException);
        }
        try {
            ChannelUtils.writeAbstractLocalPacket(localChannelReference, testPacket);
        } catch (OpenR66ProtocolPacketException e) {
            future.setResult(null);
            future.setFailure(e);
            Channels.close(localChannelReference.getLocalChannel());
            return;
        }
    }

    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(TestTransaction.class);
        }
        if (args.length < 1) {
            logger
                    .error("Needs at least the configuration file as first argument");
            return;
        }
        if (! FileBasedConfiguration
                .setClientConfigurationFromXml(args[0])) {
            logger
                    .error("Needs a correct configuration file as first argument");
            return;
        }
        Configuration.configuration.pipelineInit();

        final NetworkTransaction networkTransaction = new NetworkTransaction();
        final SocketAddress socketServerAddress = new InetSocketAddress(
                Configuration.configuration.SERVER_PORT);
        ExecutorService executorService = Executors.newCachedThreadPool();
        int nb = 100;

        R66Future[] arrayFuture = new R66Future[nb];
        logger.warn("Start Test of Transaction");
        long time1 = System.currentTimeMillis();
        for (int i = 0; i < nb; i ++) {
            arrayFuture[i] = new R66Future(true);
            TestPacket packet = new TestPacket("Test", "" + i, 0);
            TestTransaction transaction = new TestTransaction(
                    networkTransaction, arrayFuture[i], socketServerAddress,
                    packet);
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
        logger.warn("Success: " + success + " Error: " + error + " NB/s: " +
                success * TestPacket.pingpong * 1000 / (time2 - time1));
        executorService.shutdown();
        networkTransaction.closeAll();
    }

}
