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
package openr66.commander;

import goldengate.common.database.data.AbstractDbData.UpdatedInfo;
import goldengate.common.database.exception.GoldenGateDatabaseException;
import goldengate.common.database.exception.GoldenGateDatabaseNoConnectionException;
import goldengate.common.database.exception.GoldenGateDatabaseSqlException;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import openr66.database.DbConstant;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.networkhandler.NetworkTransaction;

/**
 * This class launch and control the Commander and enable TaskRunner job submissions
 *
 * @author Frederic Bregier
 *
 */
public class InternalRunner {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(InternalRunner.class);

    private final ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture<?> scheduledFuture;
    private CommanderInterface commander = null;
    private volatile boolean isRunning = true;
    private final ThreadPoolExecutor threadPoolExecutor;
    private final BlockingQueue<Runnable> workQueue;
    private final NetworkTransaction networkTransaction;

    /**
     * Create the structure to enable submission by database
     * @throws GoldenGateDatabaseNoConnectionException
     * @throws GoldenGateDatabaseSqlException
     */
    public InternalRunner() throws GoldenGateDatabaseNoConnectionException, GoldenGateDatabaseSqlException {
        if (DbConstant.admin.isConnected) {
            commander = new Commander(this, true);
        } else {
            commander = new CommanderNoDb(this, true);
        }
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        isRunning = true;
        workQueue = new ArrayBlockingQueue<Runnable>(10);
        threadPoolExecutor = new ThreadPoolExecutor(10, Configuration.configuration.RUNNER_THREAD,
                1000, TimeUnit.MILLISECONDS, workQueue);
        scheduledFuture = scheduledExecutorService.scheduleWithFixedDelay(commander,
                Configuration.configuration.delayCommander,
                Configuration.configuration.delayCommander, TimeUnit.MILLISECONDS);
        networkTransaction = new NetworkTransaction();
    }
    public NetworkTransaction getNetworkTransaction() {
        return networkTransaction;
    }
    /**
     * Submit a task
     * @param taskRunner
     */
    public void submitTaskRunner(DbTaskRunner taskRunner) {
        if (isRunning || !Configuration.configuration.isShutdown) {
            if (threadPoolExecutor.getActiveCount()+5 > Configuration.configuration.RUNNER_THREAD) {
                // too many current active threads
                taskRunner.changeUpdatedInfo(UpdatedInfo.TOSUBMIT);
                try {
                    taskRunner.update();
                } catch (GoldenGateDatabaseException e) {
                }
                return;
            }
            logger.debug("Will run {}",taskRunner);
            ClientRunner runner = new ClientRunner(networkTransaction, taskRunner, null);
            if (taskRunner.isSendThrough() && (taskRunner.isRescheduledTransfer()
                    || taskRunner.isPreTaskStarting())) {
                runner.setSendThroughMode();
                taskRunner.checkThroughMode();
            }
            runner.setDaemon(true);
            // create the client, connect and run
            threadPoolExecutor.execute(runner);
            runner = null;
        }
    }
    /**
     * First step while shutting down the service
     */
    public void prepareStopInternalRunner() {
        isRunning = false;
        scheduledFuture.cancel(false);
        scheduledExecutorService.shutdown();
        threadPoolExecutor.shutdown();
    }
    /**
     * This should be called when the server is shutting down, after stopping active requests
     * if possible.
     */
    public void stopInternalRunner() {
        isRunning = false;
        logger.info("Stopping Commander and Runner Tasks");
        scheduledFuture.cancel(false);
        scheduledExecutorService.shutdownNow();
        threadPoolExecutor.shutdownNow();
        networkTransaction.closeAll();
    }
    public int nbInternalRunner() {
        return threadPoolExecutor.getActiveCount();
    }
    public void reloadInternalRunner()
    throws GoldenGateDatabaseNoConnectionException, GoldenGateDatabaseSqlException {
        scheduledFuture.cancel(false);
        if (commander != null) {
            commander.finalize();
        }
        if (DbConstant.admin.isConnected) {
            commander = new Commander(this);
        } else {
            commander = new CommanderNoDb(this);
        }
        scheduledFuture = scheduledExecutorService.scheduleWithFixedDelay(commander,
                Configuration.configuration.delayCommander,
                Configuration.configuration.delayCommander, TimeUnit.MILLISECONDS);
    }
}
