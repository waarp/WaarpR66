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

import java.util.LinkedList;

import goldengate.common.database.data.AbstractDbData;
import goldengate.common.database.data.AbstractDbData.UpdatedInfo;
import goldengate.common.database.exception.GoldenGateDatabaseException;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.database.data.DbConfiguration;
import openr66.database.data.DbHostAuth;
import openr66.database.data.DbRule;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.utils.OpenR66SignalHandler;

/**
 * Commander is responsible to read list of updated data from time to time in order to
 * achieve new runner or new configuration updates.
 *
 * Based on no Database support
 *
 * @author Frederic Bregier
 *
 */
public class CommanderNoDb implements CommanderInterface {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(CommanderNoDb.class);

    private InternalRunner internalRunner = null;
    public static final LinkedList<AbstractDbData> todoList = new LinkedList<AbstractDbData>();

    /**
     * Prepare requests that will be executed from time to time
     * @param runner
     */
    public CommanderNoDb(InternalRunner runner) {
        this.internalConstructor(runner);
    }
    /**
     * Prepare requests that will be executed from time to time
     * @param runner
     * @param fromStartup True if call from startup of the server
     */
    public CommanderNoDb(InternalRunner runner, boolean fromStartup) {
        if (fromStartup) {
            // Change RUNNING or INTERRUPTED to TOSUBMIT since they should be ready
            // XXX FIXME TO BE DONE
            //DbTaskRunner.resetToSubmit(DbConstant.admin.session);
            ClientRunner.activeRunners = new LinkedList<ClientRunner>();
        }
        this.internalConstructor(runner);
    }
    private void internalConstructor(InternalRunner runner) {
        internalRunner = runner;
    }
    /**
     * Finalize internal data
     */
    public void finalize() {
        // no since it will be reloaded
        //todoList.clear();
    }

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        Thread.currentThread().setName("OpenR66Commander");
        while (! todoList.isEmpty()) {
            try {
                AbstractDbData data = todoList.poll();
                // First check Configuration
                if (data instanceof DbConfiguration) {
                 // should be only one...
                    DbConfiguration configuration = (DbConfiguration) data;
                    if (configuration.isOwnConfiguration()) {
                        configuration.updateConfiguration();
                    }
                    configuration.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED);
                        configuration.update();
                }
                // Check HostAuthent
                else if (data instanceof DbHostAuth) {
                    DbHostAuth hostAuth = (DbHostAuth) data;
                    // Nothing to do except validate
                    hostAuth.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED);
                    hostAuth.update();
                }
                // Check Rules
                else if (data instanceof DbRule) {
                    // Nothing to do except validate
                    DbRule rule = (DbRule) data;
                    rule.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED);
                    rule.update();
                }
                // Check TaskRunner
                else if (data instanceof DbTaskRunner) {
                    DbTaskRunner taskRunner = (DbTaskRunner) data;
                    logger.debug("get a task: {}",taskRunner);
                    // Launch if possible this task
                    String key = taskRunner.getRequested()+" "+taskRunner.getRequester()+
                        " "+taskRunner.getSpecialId();
                    if (Configuration.configuration.getLocalTransaction().
                            getFromRequest(key) != null) {
                        // already running
                        continue;
                    }
                    if (taskRunner.isSelfRequested()) {
                        // cannot schedule a request where the host is the requested host
                        taskRunner.changeUpdatedInfo(UpdatedInfo.INTERRUPTED);
                        taskRunner.update();
                        continue;
                    }
                    taskRunner.changeUpdatedInfo(UpdatedInfo.RUNNING);
                    taskRunner.update();
                    internalRunner.submitTaskRunner(taskRunner);
                    try {
                        Thread.sleep(Configuration.RETRYINMS);
                    } catch (InterruptedException e) {
                    }
                    taskRunner = null;
                }
                if (OpenR66SignalHandler.isInShutdown()) {
                    // no more task to submit
                    return;
                }
            } catch (GoldenGateDatabaseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}
