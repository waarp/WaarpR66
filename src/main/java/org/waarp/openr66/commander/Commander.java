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
package org.waarp.openr66.commander;

import org.waarp.common.database.DbPreparedStatement;
import org.waarp.common.database.data.AbstractDbData;
import org.waarp.common.database.data.AbstractDbData.UpdatedInfo;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.database.exception.WaarpDatabaseNoDataException;
import org.waarp.common.database.exception.WaarpDatabaseSqlException;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbConfiguration;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.database.data.DbHostConfiguration;
import org.waarp.openr66.database.data.DbMultipleMonitor;
import org.waarp.openr66.database.data.DbRule;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.utils.R66ShutdownHook;

/**
 * Commander is responsible to read from database updated data from time to time in order to achieve
 * new runner or new configuration updates.
 * 
 * @author Frederic Bregier
 * 
 */
public class Commander implements CommanderInterface {
    /**
     * Internal Logger
     */
    private static final WaarpLogger logger = WaarpLoggerFactory
            .getLogger(Commander.class);

    private static final int LIMITSUBMIT = 100;

    private InternalRunner internalRunner = null;
    private DbPreparedStatement preparedStatementLock = null;
    private DbPreparedStatement preparedStatementConfig = null;
    private DbPreparedStatement preparedStatementHostConfig = null;
    private DbPreparedStatement preparedStatementHost = null;
    private DbPreparedStatement preparedStatementRule = null;
    private DbPreparedStatement preparedStatementRunner = null;

    /**
     * Prepare requests that will be executed from time to time
     * 
     * @param runner
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     */
    public Commander(InternalRunner runner)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        this.internalConstructor(runner);
    }

    /**
     * Prepare requests that will be executed from time to time
     * 
     * @param runner
     * @param fromStartup
     *            True if call from startup of the server
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     */
    public Commander(InternalRunner runner, boolean fromStartup)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        this.internalConstructor(runner);
        if (fromStartup) {
            // Change RUNNING or INTERRUPTED to TOSUBMIT since they should be ready
            DbTaskRunner.resetToSubmit(DbConstant.admin.getSession());
        }
    }

    private void internalConstructor(InternalRunner runner)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        try {
            if (Configuration.configuration.getMultipleMonitors() > 1) {
                preparedStatementLock =
                        DbMultipleMonitor
                                .getUpdatedPrepareStament(DbConstant.noCommitAdmin.getSession());
            } else {
                preparedStatementLock = null;
            }
            preparedStatementConfig =
                    DbConfiguration.getUpdatedPrepareStament(DbConstant.admin.getSession());
            preparedStatementHostConfig =
                    DbHostConfiguration.getUpdatedPrepareStament(DbConstant.admin.getSession());
            preparedStatementHost =
                    DbHostAuth.getUpdatedPrepareStament(DbConstant.admin.getSession());
            preparedStatementRule =
                    DbRule.getUpdatedPrepareStament(DbConstant.admin.getSession());


            preparedStatementRunner =
                    DbTaskRunner.getSelectFromInfoPrepareStatement(DbConstant.admin.getSession(),
                            UpdatedInfo.TOSUBMIT, false, LIMITSUBMIT);

            // Clean tasks (CompleteOK and ALLDONE => DONE)
            DbTaskRunner.changeFinishedToDone(DbConstant.admin.getSession());
            internalRunner = runner;
        } finally {
            if (internalRunner == null) {
                // An error occurs
                if (preparedStatementLock != null) {
                    preparedStatementLock.realClose();
                }
                if (preparedStatementConfig != null) {
                    preparedStatementConfig.realClose();
                }
                if (preparedStatementHostConfig != null) {
                    preparedStatementHostConfig.realClose();
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
            } else {
                if (preparedStatementLock != null) {
                    DbConstant.noCommitAdmin.getSession()
                            .addLongTermPreparedStatement(preparedStatementLock);
                }
                if (preparedStatementConfig != null) {
                    DbConstant.admin.getSession().addLongTermPreparedStatement(preparedStatementConfig);
                }
                if (preparedStatementHostConfig != null) {
                    DbConstant.admin.getSession().addLongTermPreparedStatement(preparedStatementHostConfig);
                }
                if (preparedStatementHost != null) {
                    DbConstant.admin.getSession().addLongTermPreparedStatement(preparedStatementHost);
                }
                if (preparedStatementRule != null) {
                    DbConstant.admin.getSession().addLongTermPreparedStatement(preparedStatementRule);
                }
                if (preparedStatementRunner != null) {
                    DbConstant.admin.getSession().addLongTermPreparedStatement(preparedStatementRunner);
                }
            }
        }
    }

    /**
     * Finalize internal data
     */
    public void finalize() {
        if (preparedStatementLock != null) {
            try {
                DbConstant.noCommitAdmin.getSession().commit();
            } catch (WaarpDatabaseSqlException e) {
            } catch (WaarpDatabaseNoConnectionException e) {
            }
            preparedStatementLock.realClose();
            DbConstant.noCommitAdmin.getSession()
                    .removeLongTermPreparedStatements(preparedStatementLock);
            // DbConstant.noCommitAdmin.session.removeLongTermPreparedStatements();
        }
        if (preparedStatementConfig != null) {
            preparedStatementConfig.realClose();
            DbConstant.admin.getSession().removeLongTermPreparedStatements(preparedStatementConfig);
        }
        if (preparedStatementHostConfig != null) {
            preparedStatementHostConfig.realClose();
            DbConstant.admin.getSession().removeLongTermPreparedStatements(preparedStatementHostConfig);
        }
        if (preparedStatementHost != null) {
            preparedStatementHost.realClose();
            DbConstant.admin.getSession().removeLongTermPreparedStatements(preparedStatementHost);
        }
        if (preparedStatementRule != null) {
            preparedStatementRule.realClose();
            DbConstant.admin.getSession().removeLongTermPreparedStatements(preparedStatementRule);
        }
        if (preparedStatementRunner != null) {
            preparedStatementRunner.realClose();
            DbConstant.admin.getSession().removeLongTermPreparedStatements(preparedStatementRunner);
        }
        // DbConstant.admin.session.removeLongTermPreparedStatements();
    }

    public void run() {
        Thread.currentThread().setName("OpenR66Commander");
        if (DbConstant.admin.getSession() != null && DbConstant.admin.getSession().isDisActive()) {
            DbConstant.admin.getSession().checkConnectionNoException();
        }
        // each time it is runned, it parses all database for updates
        DbMultipleMonitor multipleMonitor = null;
        // Open a lock to prevent other "HA" monitors to retrieve access as Commander
        try {
            try {
                if (preparedStatementLock != null) {
                    preparedStatementLock.executeQuery();
                    preparedStatementLock.getNext();
                    multipleMonitor =
                            DbMultipleMonitor.getFromStatement(preparedStatementLock);
                }
            } catch (WaarpDatabaseNoConnectionException e) {
                logger.error("Database No Connection Error: Cannot execute Commander", e);
                try {
                    DbConstant.noCommitAdmin.getDbModel().validConnection(DbConstant.noCommitAdmin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                return;
            } catch (WaarpDatabaseSqlException e) {
                logger.error("Database SQL Error: Cannot execute Commander", e);
                try {
                    DbConstant.noCommitAdmin.getDbModel().validConnection(DbConstant.noCommitAdmin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                return;
            }
            logger.debug("Before " + multipleMonitor);
            // First check Configuration
            try {
                preparedStatementConfig.executeQuery();
                while (preparedStatementConfig.getNext()) {
                    // should be only one...
                    DbConfiguration configuration = DbConfiguration
                            .getFromStatement(preparedStatementConfig);
                    if (configuration.isOwnConfiguration()) {
                        configuration.updateConfiguration();
                    }
                    if (multipleMonitor != null) {
                        // update the configuration in HA mode
                        if (multipleMonitor.checkUpdateConfig()) {
                            configuration.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED);
                            configuration.update();
                            logger.debug("Config " + multipleMonitor);
                        } else {
                            configuration.update();
                            logger.debug("Config " + multipleMonitor);
                        }
                    } else {
                        configuration.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED);
                        configuration.update();
                    }
                    configuration = null;
                }
                preparedStatementConfig.close();
            } catch (WaarpDatabaseNoConnectionException e) {
                try {
                    DbConstant.admin.getDbModel().validConnection(DbConstant.admin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                logger.error("Database No Connection Error: Cannot execute Commander", e);
                return;
            } catch (WaarpDatabaseSqlException e) {
                try {
                    DbConstant.admin.getDbModel().validConnection(DbConstant.admin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                logger.error("Database SQL Error: Cannot execute Commander", e);
                return;
            } catch (WaarpDatabaseException e) {
                try {
                    DbConstant.admin.getDbModel().validConnection(DbConstant.admin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                logger.error("Database Error: Cannot execute Commander", e);
                return;
            } finally {
                preparedStatementConfig.close();
            }
            // check HostConfiguration
            try {
                preparedStatementHostConfig.executeQuery();
                while (preparedStatementHostConfig.getNext()) {
                    // should be only one...
                    DbHostConfiguration configuration = DbHostConfiguration
                            .getFromStatement(preparedStatementHostConfig);
                    if (configuration.isOwnConfiguration()) {
                        configuration.updateConfiguration();
                    }
                    if (multipleMonitor != null) {
                        // update the configuration in HA mode
                        if (multipleMonitor.checkUpdateConfig()) {
                            configuration.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED);
                            configuration.update();
                            logger.debug("Config " + multipleMonitor);
                        } else {
                            configuration.update();
                            logger.debug("Config " + multipleMonitor);
                        }
                    } else {
                        configuration.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED);
                        configuration.update();
                    }
                    configuration = null;
                }
                preparedStatementHostConfig.close();
            } catch (WaarpDatabaseNoConnectionException e) {
                try {
                    DbConstant.admin.getDbModel().validConnection(DbConstant.admin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                logger.error("Database No Connection Error: Cannot execute Commander", e);
                return;
            } catch (WaarpDatabaseSqlException e) {
                try {
                    DbConstant.admin.getDbModel().validConnection(DbConstant.admin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                logger.error("Database SQL Error: Cannot execute Commander", e);
                // XXX no return since table might not be initialized return;
            } catch (WaarpDatabaseException e) {
                try {
                    DbConstant.admin.getDbModel().validConnection(DbConstant.admin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                logger.error("Database Error: Cannot execute Commander", e);
                // XXX no return since table might not be initialized return;
            } finally {
                preparedStatementHostConfig.close();
            }
            // Check HostAuthent
            try {
                preparedStatementHost.executeQuery();
                boolean mm = false;
                boolean lastUpdate = false;
                while (preparedStatementHost.getNext()) {
                    // Maybe multiple
                    DbHostAuth hostAuth = DbHostAuth.getFromStatement(preparedStatementHost);
                    if (multipleMonitor != null) {
                        if (!mm) {
                            // not already set from a previous hostAuth
                            mm = true;
                            lastUpdate = multipleMonitor.checkUpdateHost();
                        } // else already set so no action on multipleMonitor

                        // Update the Host configuration in HA mode
                        if (lastUpdate) {
                            hostAuth.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED);
                            hostAuth.update();
                            logger.debug("Host " + multipleMonitor);
                        } else {
                            // Nothing to do except validate
                            hostAuth.update();
                            logger.debug("Host " + multipleMonitor);
                        }
                    } else {
                        // Nothing to do except validate
                        hostAuth.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED);
                        hostAuth.update();
                    }
                    hostAuth = null;
                }
            } catch (WaarpDatabaseNoConnectionException e) {
                try {
                    DbConstant.admin.getDbModel().validConnection(DbConstant.admin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                logger.error("Database No Connection Error: Cannot execute Commander", e);
                return;
            } catch (WaarpDatabaseSqlException e) {
                try {
                    DbConstant.admin.getDbModel().validConnection(DbConstant.admin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                logger.error("Database SQL Error: Cannot execute Commander", e);
                return;
            } catch (WaarpDatabaseException e) {
                try {
                    DbConstant.admin.getDbModel().validConnection(DbConstant.admin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                logger.error("Database Error: Cannot execute Commander", e);
                return;
            } finally {
                preparedStatementHost.close();
            }
            // Check Rules
            try {
                preparedStatementRule.executeQuery();
                boolean mm = false;
                boolean lastUpdate = false;
                while (preparedStatementRule.getNext()) {
                    DbRule rule = DbRule.getFromStatement(preparedStatementRule);
                    if (multipleMonitor != null) {
                        if (!mm) {
                            // not already set from a previous hostAuth
                            mm = true;
                            lastUpdate = multipleMonitor.checkUpdateRule();
                        } // else already set so no action on multipleMonitor
                          // Update the Rules in HA mode
                        if (lastUpdate) {
                            rule.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED);
                            rule.update();
                            logger.debug("Rule " + multipleMonitor);
                        } else {
                            // Nothing to do except validate
                            rule.update();
                            logger.debug("Rule " + multipleMonitor);
                        }
                    } else {
                        // Nothing to do except validate
                        rule.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED);
                        rule.update();
                    }
                    rule = null;
                }
            } catch (WaarpDatabaseNoConnectionException e) {
                try {
                    DbConstant.admin.getDbModel().validConnection(DbConstant.admin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                logger.error("Database No Connection Error: Cannot execute Commander", e);
                return;
            } catch (WaarpDatabaseSqlException e) {
                try {
                    DbConstant.admin.getDbModel().validConnection(DbConstant.admin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                logger.error("Database SQL Error: Cannot execute Commander", e);
                return;
            } catch (WaarpDatabaseNoDataException e) {
                try {
                    DbConstant.admin.getDbModel().validConnection(DbConstant.admin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                logger.error("Database Error: Cannot execute Commander", e);
                return;
            } catch (WaarpDatabaseException e) {
                try {
                    DbConstant.admin.getDbModel().validConnection(DbConstant.admin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                logger.error("Database Error: Cannot execute Commander", e);
                return;
            } finally {
                preparedStatementRule.close();
            }
            if (R66ShutdownHook.isShutdownStarting()) {
                // no more task to submit
                return;
            }

            // Lauch Transfer ready to be submited
            logger.debug("start runner");
            try {
                DbTaskRunner.finishSelectOrCountPrepareStatement(preparedStatementRunner);
                // No specific HA mode since the other servers will wait for the commit on Lock
                preparedStatementRunner.executeQuery();
                while (preparedStatementRunner.getNext()) {
                    if (R66ShutdownHook.isShutdownStarting()) {
                        logger.info("Will not start transfers, server is in shutdown.");
                        return;
                    }
                    DbTaskRunner taskRunner = null;
                    try {
                        taskRunner = DbTaskRunner
                                .getFromStatement(preparedStatementRunner);
                    } catch (WaarpDatabaseSqlException e) {
                        // ignore and continue if NoData
                        if (e.getCause() instanceof WaarpDatabaseNoDataException) {
                            logger.warn("DbTaskRunner cannot be loaded: " + e.getMessage());
                            continue;
                        }
                        throw e;
                    }
                    logger.debug("get a task: {}", taskRunner);
                    // Launch if possible this task
                    String key = taskRunner.getRequested() + " " + taskRunner.getRequester() +
                            " " + taskRunner.getSpecialId();
                    if (Configuration.configuration.getLocalTransaction().
                            getFromRequest(key) != null) {
                        // already running
                        continue;
                    }
                    if (taskRunner.isSelfRequested()) {
                        // cannot schedule a request where the host is the requested host
                        taskRunner.changeUpdatedInfo(UpdatedInfo.INTERRUPTED);
                        try {
                            taskRunner.update();
                        } catch (WaarpDatabaseException e) {
                            if (e instanceof WaarpDatabaseNoDataException) {
                                logger.warn("Update failed, no transfer found");
                                continue;
                            } else {
                                logger.error("Database Error: Cannot execute Commander", e);
                                DbConstant.admin.getDbModel().validConnection(
                                        DbConstant.admin.getSession());
                                return;
                            }
                        }
                        continue;
                    }
                    internalRunner.submitTaskRunner(taskRunner);
                    taskRunner = null;
                }
            } catch (WaarpDatabaseNoConnectionException e) {
                try {
                    DbConstant.admin.getDbModel().validConnection(DbConstant.admin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                logger.error("Database No Connection Error: Cannot execute Commander", e);
                return;
            } catch (WaarpDatabaseSqlException e) {
                try {
                    DbConstant.admin.getDbModel().validConnection(DbConstant.admin.getSession());
                } catch (WaarpDatabaseNoConnectionException e1) {
                }
                logger.error("Database SQL Error: Cannot execute Commander", e);
                return;
            } finally {
                preparedStatementRunner.close();
            }
            logger.debug("end commander");
        } finally {
            if (multipleMonitor != null) {
                try {
                    // Now update and Commit so releasing the lock
                    logger.debug("Update " + multipleMonitor);
                    multipleMonitor.update();
                    DbConstant.noCommitAdmin.getSession().commit();
                } catch (WaarpDatabaseException e) {
                    try {
                        DbConstant.noCommitAdmin.getDbModel().validConnection(DbConstant.noCommitAdmin.getSession());
                    } catch (WaarpDatabaseNoConnectionException e1) {
                    }
                }
                multipleMonitor = null;
            }
        }
    }

}
