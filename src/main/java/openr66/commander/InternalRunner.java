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

import goldengate.common.database.exception.OpenR66DatabaseException;
import goldengate.common.database.exception.OpenR66DatabaseNoConnectionError;
import goldengate.common.database.exception.OpenR66DatabaseSqlError;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
    private Commander commander = null;
    private volatile boolean isRunning = true;
    private final ThreadPoolExecutor threadPoolExecutor;
    private final BlockingQueue<Runnable> workQueue;
    private final NetworkTransaction networkTransaction;

    /**
     * Create the structure to enable submission by database
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public InternalRunner() throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        commander = new Commander(this, true);
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
     * @throws OpenR66DatabaseException
     */
    public void submitTaskRunner(DbTaskRunner taskRunner) throws OpenR66DatabaseException {
        if (isRunning) {
            logger.debug("Will run {}",taskRunner);
            ClientRunner runner = new ClientRunner(networkTransaction, taskRunner, null);
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
    public void reloadInternalRunner()
    throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        scheduledFuture.cancel(false);
        if (commander != null) {
            commander.finalize();
        }
        commander = new Commander(this);
        scheduledFuture = scheduledExecutorService.scheduleWithFixedDelay(commander,
                Configuration.configuration.delayCommander,
                Configuration.configuration.delayCommander, TimeUnit.MILLISECONDS);
    }
}
