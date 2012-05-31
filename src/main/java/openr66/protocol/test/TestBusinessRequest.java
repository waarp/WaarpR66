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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import openr66.client.AbstractBusinessRequest;
import openr66.configuration.FileBasedConfiguration;
import openr66.database.data.DbHostAuth;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.localhandler.packet.BusinessRequestPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.logging.InternalLoggerFactory;

/**
 * Test class for internal Business test
 * @author Frederic Bregier
 *
 */
public class TestBusinessRequest extends AbstractBusinessRequest {
    /**
     * Internal Logger
     */
    private static GgInternalLogger logger;

    public TestBusinessRequest(NetworkTransaction networkTransaction,
            R66Future future, String remoteHost, BusinessRequestPacket packet) {
        super(TestBusinessRequest.class, future, remoteHost, networkTransaction, packet);
    }

    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                null));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(TestBusinessRequest.class);
        }
        if (args.length < 1) {
            logger
                    .error("Needs at least the configuration file as first argument");
            return;
        }
        if (! FileBasedConfiguration
                .setClientConfigurationFromXml(Configuration.configuration, args[0])) {
            logger
                    .error("Needs a correct configuration file as first argument");
            return;
        }
        Configuration.configuration.pipelineInit();

        final NetworkTransaction networkTransaction = new NetworkTransaction();
        DbHostAuth host = Configuration.configuration.HOST_AUTH;
        ExecutorService executorService = Executors.newCachedThreadPool();
        int nb = 100;

        R66Future[] arrayFuture = new R66Future[nb];
        logger.info("Start Test of Transaction");
        long time1 = System.currentTimeMillis();
        for (int i = 0; i < nb; i ++) {
            arrayFuture[i] = new R66Future(true);
            BusinessRequestPacket packet = new BusinessRequestPacket(TestExecJavaTask.class.getName()+" business 0 other arguments", 0);
            TestBusinessRequest transaction = new TestBusinessRequest(
                    networkTransaction, arrayFuture[i], host.getHostid(),
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
                success * 100 * 1000 / (time2 - time1));
        executorService.shutdown();
        networkTransaction.closeAll();
    }

}
