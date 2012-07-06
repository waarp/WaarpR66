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
package org.waarp.openr66.protocol.test;

import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.netty.channel.Channels;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.configuration.FileBasedConfiguration;
import org.waarp.openr66.context.R66FiniteDualStates;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66Exception;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNetworkException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;
import org.waarp.openr66.protocol.localhandler.packet.TestPacket;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;

/**
 * Test class for internal ping pong test
 * 
 * @author Frederic Bregier
 * 
 */
public class TestTransaction implements Runnable {
	/**
	 * Internal Logger
	 */
	private static WaarpInternalLogger logger;

	final private NetworkTransaction networkTransaction;

	final private R66Future future;

	private final SocketAddress socketAddress;

	final private TestPacket testPacket;

	public TestTransaction(NetworkTransaction networkTransaction,
			R66Future future, SocketAddress socketAddress, TestPacket packet) {
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(TestTransaction.class);
		}
		this.networkTransaction = networkTransaction;
		this.future = future;
		this.socketAddress = socketAddress;
		testPacket = packet;
	}

	public void run() {
		LocalChannelReference localChannelReference = null;
		OpenR66Exception lastException = null;
		for (int i = 0; i < Configuration.RETRYNB; i++) {
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
			future.setFailure(lastException);
			return;
		} else if (lastException != null) {
			logger.info("Connection retry since ", lastException);
		}
		localChannelReference.sessionNewState(R66FiniteDualStates.TEST);
		try {
			ChannelUtils.writeAbstractLocalPacket(localChannelReference, testPacket, false);
		} catch (OpenR66ProtocolPacketException e) {
			future.setResult(null);
			future.setFailure(e);
			Channels.close(localChannelReference.getLocalChannel());
			return;
		}
	}

	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(
				null));
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(TestTransaction.class);
		}
		if (args.length < 1) {
			logger
					.error("Needs at least the configuration file as first argument");
			return;
		}
		if (!FileBasedConfiguration
				.setClientConfigurationFromXml(Configuration.configuration, args[0])) {
			logger
					.error("Needs a correct configuration file as first argument");
			return;
		}
		Configuration.configuration.pipelineInit();

		final NetworkTransaction networkTransaction = new NetworkTransaction();
		DbHostAuth host = Configuration.configuration.HOST_AUTH;
		final SocketAddress socketServerAddress = host.getSocketAddress();
		ExecutorService executorService = Executors.newCachedThreadPool();
		int nb = 100;

		R66Future[] arrayFuture = new R66Future[nb];
		logger.info("Start Test of Transaction");
		long time1 = System.currentTimeMillis();
		for (int i = 0; i < nb; i++) {
			arrayFuture[i] = new R66Future(true);
			TestPacket packet = new TestPacket("Test", "" + i, 0);
			TestTransaction transaction = new TestTransaction(
					networkTransaction, arrayFuture[i], socketServerAddress,
					packet);
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
		logger.warn("Success: " + success + " Error: " + error + " NB/s: " +
				success * TestPacket.pingpong * 1000 / (time2 - time1));
		executorService.shutdown();
		networkTransaction.closeAll();
	}

}
