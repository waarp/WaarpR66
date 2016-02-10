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

import org.waarp.common.database.DbPreparedStatement;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.database.exception.WaarpDatabaseSqlException;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.configuration.FileBasedConfiguration;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.database.data.DbRule;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.Messages;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessException;

/**
 * Test class for JsonGenerator
 * 
 * @author Frederic Bregier
 * 
 */
public class TestJsonGenerator {
    /**
     * Internal Logger
     */
    static protected volatile WaarpLogger logger;
    static int nb = 11;

    /**
     * @param args
     * @param rank
     * @return True if OK
     */
    protected static boolean getSpecialParams(String[] args, int rank) {
        for (int i = rank; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-nb")) {
                i++;
                nb = Integer.parseInt(args[i]);
            }
        }
        return true;
    }

    public static void main(String[] args) {
        WaarpLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(
                null));
        if (logger == null) {
            logger = WaarpLoggerFactory.getLogger(TestJsonGenerator.class);
        }
        if (args.length == 0 || !FileBasedConfiguration
                .setSubmitClientConfigurationFromXml(Configuration.configuration, args[0])) {
            logger.error(Messages.getString("Configuration.NeedCorrectConfig")); //$NON-NLS-1$
            logger.error("Wrong initialization");
            if (DbConstant.admin != null && DbConstant.admin.isActive()) {
                DbConstant.admin.close();
            }
            System.exit(1);
        }
        getSpecialParams(args, 1);
        logger.warn("Start Test Json");
        DbPreparedStatement preparedStatement;
        try {
            preparedStatement = DbTaskRunner.getFilterPrepareStatement(DbConstant.admin.getSession(), nb, false,
                    null, null, null, null, null, null,
                    false, false, false, false, true, null);
            preparedStatement.executeQuery();
            String tasks = DbTaskRunner.getJson(preparedStatement, nb);
            System.out.println(tasks);
            preparedStatement.realClose();
        } catch (WaarpDatabaseNoConnectionException e) {
            e.printStackTrace();
        } catch (WaarpDatabaseSqlException e) {
            e.printStackTrace();
        } catch (OpenR66ProtocolBusinessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println();
        try {
            preparedStatement = DbHostAuth.getFilterPrepareStament(DbConstant.admin.getSession(),
                    null, null);
            preparedStatement.executeQuery();
            String hosts = DbHostAuth.getJson(preparedStatement, nb);
            System.out.println(hosts);
            preparedStatement.realClose();
        } catch (WaarpDatabaseNoConnectionException e) {
            e.printStackTrace();
        } catch (WaarpDatabaseSqlException e) {
            e.printStackTrace();
        } catch (OpenR66ProtocolBusinessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println();
        try {
            preparedStatement = DbRule.getFilterPrepareStament(DbConstant.admin.getSession(),
                    null, -1);
            preparedStatement.executeQuery();
            String rules = DbRule.getJson(preparedStatement, nb);
            System.out.println(rules);
            preparedStatement.realClose();
        } catch (WaarpDatabaseNoConnectionException e) {
            e.printStackTrace();
        } catch (WaarpDatabaseSqlException e) {
            e.printStackTrace();
        } catch (OpenR66ProtocolBusinessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
