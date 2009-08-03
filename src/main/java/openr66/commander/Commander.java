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

            String request = "SELECT " +DbConfiguration.selectAllFields;
            request += " FROM "+DbConfiguration.table+
                " WHERE "+DbConfiguration.Columns.UPDATEDINFO.name()+" = "+
                AbstractDbData.UpdatedInfo.UPDATED.ordinal();
            preparedStatementConfig.createPrepareStatement(request);

            request = "SELECT " +DbHostAuth.selectAllFields;
            request += " FROM "+DbHostAuth.table+
                " WHERE "+DbConfiguration.Columns.UPDATEDINFO.name()+" = "+
                AbstractDbData.UpdatedInfo.UPDATED.ordinal();
            preparedStatementHost.createPrepareStatement(request);

            request = "SELECT " +DbRule.selectAllFields;
            request += " FROM "+DbRule.table+
                " WHERE "+DbConfiguration.Columns.UPDATEDINFO.name()+" = "+
                AbstractDbData.UpdatedInfo.UPDATED.ordinal();
            preparedStatementRule.createPrepareStatement(request);

            request = "SELECT " +DbTaskRunner.selectAllFields;
            request += " FROM "+DbTaskRunner.table+
                " WHERE "+DbConfiguration.Columns.UPDATEDINFO.name()+" = "+
                AbstractDbData.UpdatedInfo.UPDATED.ordinal();
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
        logger.info("start config");
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
            logger.error("Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseSqlError e) {
            logger.error("Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseException e) {
            logger.error("Cannot execute Commander", e);
            return;
        } finally {
            preparedStatementConfig.close();
        }
        logger.info("start host");
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
            logger.error("Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseSqlError e) {
            logger.error("Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseException e) {
            logger.error("Cannot execute Commander", e);
            return;
        } finally {
            preparedStatementHost.close();
        }
        logger.info("start rule");
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
            logger.error("Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseSqlError e) {
            logger.error("Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseNoDataException e) {
            logger.error("Cannot execute Commander", e);
            return;
        } finally {
            preparedStatementRule.close();
        }
        logger.info("start runner");
        // Check TaskRunner
        try {
            preparedStatementRunner.executeQuery();
            while (preparedStatementRunner.getNext()) {
                DbTaskRunner taskRunner = DbTaskRunner.getFromStatement(preparedStatementRunner);
                logger.info("get a task: "+taskRunner.toString());
                // Launch if possible this task
                internalRunner.submitTaskRunner(taskRunner);
                taskRunner = null;
            }
        } catch (OpenR66DatabaseNoConnectionError e) {
            logger.error("Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseSqlError e) {
            logger.error("Cannot execute Commander", e);
            return;
        } catch (OpenR66DatabaseException e) {
            logger.error("Cannot execute Commander", e);
            return;
        } finally {
            preparedStatementRunner.close();
        }
        logger.info("end commander");
    }

}
