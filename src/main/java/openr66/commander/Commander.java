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

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.database.DbConstant;
import openr66.database.DbPreparedStatement;
import openr66.database.data.AbstractDbData;
import openr66.database.data.DbConfiguration;
import openr66.database.data.DbHostAuth;
import openr66.database.data.DbRule;
import openr66.database.data.DbTaskRunner;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseNoDataException;
import openr66.database.exception.OpenR66DatabaseSqlError;

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

    private InternalRunner internalRunner = null;
    private DbPreparedStatement preparedStatementConfig = null;
    private DbPreparedStatement preparedStatementHost = null;
    private DbPreparedStatement preparedStatementRule = null;
    private DbPreparedStatement preparedStatementRunner = null;

    public Commander(InternalRunner runner)
        throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        try {
            preparedStatementConfig = new DbPreparedStatement(
                    DbConstant.admin.session);
            preparedStatementHost = new DbPreparedStatement(
                    DbConstant.admin.session);
            preparedStatementRule = new DbPreparedStatement(
                    DbConstant.admin.session);
            preparedStatementRunner = new DbPreparedStatement(
                    DbConstant.admin.session);

            DbConfiguration.Columns [] columnsConf = DbConfiguration.Columns.values();
            String request = "SELECT " +columnsConf[0].name();
            for (int i = 1; i < columnsConf.length; i++) {
                request += ","+columnsConf[i].name();
            }
            request += " FROM "+DbConfiguration.table+
                " WHERE UPDATEDINFO = "+AbstractDbData.UpdatedInfo.UPDATED.ordinal();
            preparedStatementConfig.createPrepareStatement(request);

            DbHostAuth.Columns [] columnsHost = DbHostAuth.Columns.values();
            request = "SELECT " +columnsHost[0].name();
            for (int i = 1; i < columnsHost.length; i++) {
                request += ","+columnsHost[i].name();
            }
            request += " FROM "+DbHostAuth.table+
                " WHERE UPDATEDINFO = "+AbstractDbData.UpdatedInfo.UPDATED.ordinal();
            preparedStatementHost.createPrepareStatement(request);

            DbRule.Columns [] columnsRule = DbRule.Columns.values();
            request = "SELECT " +columnsRule[0].name();
            for (int i = 1; i < columnsRule.length; i++) {
                request += ","+columnsRule[i].name();
            }
            request += " FROM "+DbRule.table+
                " WHERE UPDATEDINFO = "+AbstractDbData.UpdatedInfo.UPDATED.ordinal();
            preparedStatementRule.createPrepareStatement(request);

            DbTaskRunner.Columns [] columnsRunner = DbTaskRunner.Columns.values();
            request = "SELECT " +columnsRunner[0].name();
            for (int i = 1; i < columnsRunner.length; i++) {
                request += ","+columnsRunner[i].name();
            }
            request += " FROM "+DbTaskRunner.table+
                " WHERE UPDATEDINFO = "+AbstractDbData.UpdatedInfo.UPDATED.ordinal();
            preparedStatementRunner.createPrepareStatement(request);
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
    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        Thread.currentThread().setName("OpenR66Commander");
        logger.warn("start config");
        // each time it is runned, it parses all database for updates
        // First check Configuration
        try {
            preparedStatementConfig.executeQuery();
            while (preparedStatementConfig.getNext()) {
                // should be only one...
                DbConfiguration configuration = DbConfiguration.getUpdated(preparedStatementConfig);
                if (configuration.isOwnConfiguration()) {
                    configuration.updateConfiguration();
                }
                configuration.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED.ordinal());
                configuration.update();
            }
            preparedStatementConfig.close();
        } catch (OpenR66DatabaseNoConnectionError e) {
            logger.error("Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseSqlError e) {
            logger.error("Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseException e) {
            logger.error("Cannot execute Commander", e);
            return;
        }
        logger.warn("start host");
        // Check HostAuthent
        try {
            preparedStatementHost.executeQuery();
            while (preparedStatementHost.getNext()) {
                DbHostAuth hostAuth = DbHostAuth.getUpdated(preparedStatementHost);
                // Nothing to do except validate
                hostAuth.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED.ordinal());
                hostAuth.update();
            }
            preparedStatementHost.close();
        } catch (OpenR66DatabaseNoConnectionError e) {
            logger.error("Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseSqlError e) {
            logger.error("Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseException e) {
            logger.error("Cannot execute Commander", e);
            return;
        }
        logger.warn("start rule");
        // Check Rules
        try {
            preparedStatementRule.executeQuery();
            while (preparedStatementRule.getNext()) {
                DbRule rule = DbRule.getUpdated(preparedStatementRule);
                // Nothing to do except validate
                rule.changeUpdatedInfo(AbstractDbData.UpdatedInfo.NOTUPDATED.ordinal());
                rule.update();
            }
            preparedStatementRule.close();
        } catch (OpenR66DatabaseNoConnectionError e) {
            logger.error("Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseSqlError e) {
            logger.error("Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseNoDataException e) {
            logger.error("Cannot execute Commander", e);
            return;
        }
        logger.warn("start runner");
        // Check TaskRunner
        try {
            preparedStatementRunner.executeQuery();
            while (preparedStatementRunner.getNext()) {
                DbTaskRunner taskRunner = DbTaskRunner.getUpdated(preparedStatementRunner);
                // Launch if possible this task
                internalRunner.submitTaskRunner(taskRunner);
            }
            preparedStatementRunner.close();
        } catch (OpenR66DatabaseNoConnectionError e) {
            logger.error("Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseSqlError e) {
            logger.error("Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseException e) {
            logger.error("Cannot execute Commander", e);
            return;
        }
        logger.warn("end commander");
    }

}
