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

import goldengate.common.database.DbPreparedStatement;
import goldengate.common.database.data.AbstractDbData;
import goldengate.common.database.data.AbstractDbData.UpdatedInfo;
import goldengate.common.database.exception.OpenR66DatabaseException;
import goldengate.common.database.exception.OpenR66DatabaseNoConnectionError;
import goldengate.common.database.exception.OpenR66DatabaseNoDataException;
import goldengate.common.database.exception.OpenR66DatabaseSqlError;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.database.DbConstant;
import openr66.database.data.DbConfiguration;
import openr66.database.data.DbHostAuth;
import openr66.database.data.DbRule;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.utils.OpenR66SignalHandler;

/**
 * Commander is responsible to read from database updated data from time to time in order to
 * achieve new runner or new configuration updates.
 *
 * @author Frederic Bregier
 *
 */
public class Commander implements Runnable {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(Commander.class);

    private static final int LIMITSUBMIT = 100;

    private InternalRunner internalRunner = null;
    private DbPreparedStatement preparedStatementConfig = null;
    private DbPreparedStatement preparedStatementHost = null;
    private DbPreparedStatement preparedStatementRule = null;
    private DbPreparedStatement preparedStatementRunner = null;

    /**
     * Prepare requests that will be executed from time to time
     * @param runner
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public Commander(InternalRunner runner)
        throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        this.internalConstructor(runner);
    }
    /**
     * Prepare requests that will be executed from time to time
     * @param runner
     * @param fromStartup True if call from startup of the server
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public Commander(InternalRunner runner, boolean fromStartup)
        throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        this.internalConstructor(runner);
        if (fromStartup) {
            // Change RUNNING or INTERRUPTED to TOSUBMIT since they should be ready
            DbTaskRunner.resetToSubmit(DbConstant.admin.session);
        }
    }
    private void internalConstructor(InternalRunner runner)
    throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        try {
            preparedStatementConfig =
                DbConfiguration.getUpdatedPrepareStament(DbConstant.admin.session);
            preparedStatementHost =
                DbHostAuth.getUpdatedPrepareStament(DbConstant.admin.session);
            preparedStatementRule =
                DbRule.getUpdatedPrepareStament(DbConstant.admin.session);
            preparedStatementRunner =
                DbTaskRunner.getUpdatedPrepareStament(DbConstant.admin.session,
                        UpdatedInfo.TOSUBMIT, false, LIMITSUBMIT);
            // Clean tasks (CompleteOK and ALLDONE => DONE)
            DbTaskRunner.changeFinishedToDone(DbConstant.admin.session);
            internalRunner = runner;
        } finally {
            if (internalRunner == null) {
                // An error occurs
                if (preparedStatementConfig != null) {
                    preparedStatementConfig.realClose();
                }
                if (preparedStatementHost != null) {
                    preparedStatementHost.realClose();
                }
                if (preparedStatementRule != null) {
                    preparedStatementRule.realClose();
                }
                if (preparedStatementRunner != null) {
                    preparedStatementRunner.realClose();
                }
            }
        }
    }
    /**
     * Finalize internal data
     */
    public void finalize() {
        if (preparedStatementConfig != null) {
            preparedStatementConfig.realClose();
        }
        if (preparedStatementHost != null) {
            preparedStatementHost.realClose();
        }
        if (preparedStatementRule != null) {
            preparedStatementRule.realClose();
        }
        if (preparedStatementRunner != null) {
            preparedStatementRunner.realClose();
        }
    }
    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        Thread.currentThread().setName("OpenR66Commander");
        // each time it is runned, it parses all database for updates
        // First check Configuration
        try {
            preparedStatementConfig.executeQuery();
            while (preparedStatementConfig.getNext()) {
                // should be only one...
                DbConfiguration configuration = DbConfiguration.getFromStatement(preparedStatementConfig);
                if (configuration.isOwnConfiguration()) {
                    configuration.updateConfiguration();
                }
                configuration.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED);
                configuration.update();
                configuration = null;
            }
            preparedStatementConfig.close();
        } catch (OpenR66DatabaseNoConnectionError e) {
            logger.error("Database No Connection Error: Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseSqlError e) {
            logger.error("Database SQL Error: Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseException e) {
            logger.error("Database Error: Cannot execute Commander", e);
            return;
        } finally {
            preparedStatementConfig.close();
        }
        // Check HostAuthent
        try {
            preparedStatementHost.executeQuery();
            while (preparedStatementHost.getNext()) {
                DbHostAuth hostAuth = DbHostAuth.getFromStatement(preparedStatementHost);
                // Nothing to do except validate
                hostAuth.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED);
                hostAuth.update();
                hostAuth = null;
            }
        } catch (OpenR66DatabaseNoConnectionError e) {
            logger.error("Database No Connection Error: Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseSqlError e) {
            logger.error("Database SQL Error: Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseException e) {
            logger.error("Database Error: Cannot execute Commander", e);
            return;
        } finally {
            preparedStatementHost.close();
        }
        // Check Rules
        try {
            preparedStatementRule.executeQuery();
            while (preparedStatementRule.getNext()) {
                DbRule rule = DbRule.getFromStatement(preparedStatementRule);
                // Nothing to do except validate
                rule.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED);
                rule.update();
                rule = null;
            }
        } catch (OpenR66DatabaseNoConnectionError e) {
            logger.error("Database No Connection Error: Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseSqlError e) {
            logger.error("Database SQL Error: Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseNoDataException e) {
            logger.error("Database Error: Cannot execute Commander", e);
            return;
        } finally {
            preparedStatementRule.close();
        }
        if (OpenR66SignalHandler.isInShutdown()) {
            // no more task to submit
            return;
        }
        logger.debug("start runner");
        // Check TaskRunner
        try {
            preparedStatementRunner.executeQuery();
            while (preparedStatementRunner.getNext()) {
                DbTaskRunner taskRunner = DbTaskRunner.getFromStatement(preparedStatementRunner);
                logger.info("get a task: {}",taskRunner);
                // Launch if possible this task
                String key = taskRunner.getRequested()+" "+taskRunner.getRequester()+
                    " "+taskRunner.getSpecialId();
                if (Configuration.configuration.getLocalTransaction().
                        getFromRequest(key) != null) {
                    // already running
                    continue;
                }
                taskRunner.changeUpdatedInfo(UpdatedInfo.RUNNING);
                taskRunner.update();
                internalRunner.submitTaskRunner(taskRunner);
                taskRunner = null;
            }
        } catch (OpenR66DatabaseNoConnectionError e) {
            logger.error("Database No Connection Error: Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseSqlError e) {
            logger.error("Database SQL Error: Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseException e) {
            logger.error("Database Error: Cannot execute Commander", e);
            return;
        } finally {
            preparedStatementRunner.close();
        }
        logger.debug("end commander");
    }

}
