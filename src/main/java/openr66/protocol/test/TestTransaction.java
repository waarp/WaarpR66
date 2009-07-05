/**
 * Copyright 2009, Frederic Bregier, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package openr66.protocol.test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
import openr66.protocol.localhandler.packet.TestPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.networkhandler.packet.NetworkPacket;
import openr66.protocol.utils.ChannelUtils;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;

/**
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
    final private Channel networkChannel;
    final private TestPacket testPacket;
    
    public TestTransaction(NetworkTransaction networkTransaction, R66Future future, Channel networkChannel, TestPacket packet) {
        if (logger == null) {
            logger = GgInternalLoggerFactory
                .getLogger(TestTransaction.class);
        }
        this.networkTransaction = networkTransaction;
        this.future = future;
        this.networkChannel = networkChannel;
        this.testPacket = packet;
    }
    public void run() {
        LocalChannelReference localChannelReference;
        try {
            localChannelReference = this.networkTransaction
                    .createNewClient(this.networkChannel);
        } catch (OpenR66ProtocolNetworkException e1) {
            logger.error("Cannot connect", e1);
            this.future.setResult(null);
            this.future.setFailure(e1);
            return;
        }
        NetworkPacket networkPacket;
        try {
            networkPacket = new NetworkPacket(localChannelReference.getLocalId(), ChannelUtils.NOCHANNEL, 
                    LocalPacketFactory.TESTPACKET,
                    this.testPacket.getLocalPacket());
        } catch (OpenR66ProtocolPacketException e) {
            this.future.setResult(null);
            this.future.setFailure(e);
            Channels.close(localChannelReference.getLocalChannel());
            return;
        }
        ChannelUtils.write(localChannelReference.getNetworkChannel(), networkPacket);
        localChannelReference.getFuture().awaitUninterruptibly();
        if (localChannelReference.getFuture().isSuccess()) {
            this.future.setResult(localChannelReference.getFuture().getResult());
            this.future.setSuccess();
        } else {
            this.future.setResult(null);
            this.future.setFailure(localChannelReference.getFuture().getCause());
        }
    }
    
    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(TestTransaction.class);
        }
        Configuration.configuration.computeNbThreads();
        final NetworkTransaction networkTransaction = new NetworkTransaction();
        final SocketAddress socketServerAddress = new InetSocketAddress(
                Configuration.SERVER_PORT);
        Channel channel;
        try {
            channel = networkTransaction.createNewConnection(socketServerAddress);
        } catch (OpenR66ProtocolNetworkException e) {
            logger.error("Cannot connect",e);
            return;
        }
        ExecutorService executorService = Executors.newCachedThreadPool();
        int nb = 100;
        
        R66Future []arrayFuture = new R66Future[nb];
        logger.warn("Start");
        long time1 = System.currentTimeMillis();
        for (int i = 0; i < nb; i++) {
            arrayFuture[i] = new R66Future(true);
            TestPacket packet = new TestPacket("Test", ""+i, 0);
            TestTransaction transaction = new TestTransaction(networkTransaction, arrayFuture[i], channel, packet);
            executorService.execute(transaction);
        }
        int success = 0;
        int error = 0;
        for (int i = 0; i < nb; i++) {
            arrayFuture[i].awaitUninterruptibly();
            if (arrayFuture[i].isSuccess()) {
                success++;
            } else {
                error++;
            }
        }
        long time2 = System.currentTimeMillis();
        logger.warn("Success: "+success+" Error: "+error+" NB/s: "+(success*1000/(time2-time1)));
        networkTransaction.closeAll();
        executorService.shutdown();
    }
    
}
