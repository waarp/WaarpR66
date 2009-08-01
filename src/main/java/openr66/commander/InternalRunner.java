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
package openr66.commander;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import openr66.database.data.AbstractDbData;
import openr66.database.data.DbTaskRunner;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.protocol.config.Configuration;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

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
    private final ScheduledFuture<?> scheduledFuture;
    private volatile boolean isRunning = true;
    private final ExecutorService executorService;

    public InternalRunner() throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        Commander commander = new Commander(this);
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        isRunning = true;
        executorService = Executors.newCachedThreadPool();
        scheduledFuture = scheduledExecutorService.scheduleWithFixedDelay(commander,
                Configuration.configuration.delayCommander,
                Configuration.configuration.delayCommander, TimeUnit.MILLISECONDS);
    }

    public void submitTaskRunner(DbTaskRunner taskRunner) throws OpenR66DatabaseException {
        if (isRunning) {
            logger.warn("Will run "+taskRunner.toString());
            taskRunner.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED.ordinal());
            taskRunner.update();
            // FIXME create the client, connect and run
        }
    }
    public void prepareStopInternalRunner() {
        isRunning = false;
        scheduledFuture.cancel(false);
    }
    /**
     * This should be called when the server is shutting down, after stopping active requests
     * if possible.
     */
    public void stopInternalRunner() {
        isRunning = false;
        logger.warn("Stopping Commander and Runner Tasks");
        scheduledFuture.cancel(false);
        scheduledExecutorService.shutdownNow();
        executorService.shutdownNow();
    }
}
