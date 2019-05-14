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
package org.waarp.openr66.server;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.configuration.AuthenticationFileBasedConfiguration;
import org.waarp.openr66.configuration.FileBasedConfiguration;
import org.waarp.openr66.configuration.RuleFileBasedConfiguration;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbHostConfiguration;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.Messages;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;
import org.waarp.openr66.protocol.utils.ChannelUtils;

/**
 * Utility class to initiate the database for a server
 * 
 * @author Frederic Bregier
 * 
 */
public class ServerInitDatabase {
    /**
     * Internal Logger
     */
    static volatile WaarpLogger logger;

    protected static String _INFO_ARGS =
            Messages.getString("ServerInitDatabase.Help"); //$NON-NLS-1$

    static String sxml = null;
    static boolean database = false;
    static boolean upgradeDb = false;
    static String sbusiness = null;
    static String salias = null;
    static String sroles = null;
    static String sdirconfig = null;
    static String shostauth = null;
    static String slimitconfig = null;

    protected static boolean getParams(String[] args) {
        if (args.length < 1) {
            logger.error(_INFO_ARGS);
            return false;
        }
        sxml = args[0];
        for (int i = 1; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-initdb")) {
                database = true;
                FileBasedConfiguration.autoupgrade = false;
                upgradeDb = true;
            } else if (args[i].equalsIgnoreCase("-upgradeDb")) {
                upgradeDb = true;
            } else if (args[i].equalsIgnoreCase("-loadBusiness")) {
                i++;
                sbusiness = args[i];
            } else if (args[i].equalsIgnoreCase("-loadAlias")) {
                i++;
                salias = args[i];
            } else if (args[i].equalsIgnoreCase("-loadRoles")) {
                i++;
                sroles = args[i];
            } else if (args[i].equalsIgnoreCase("-dir")) {
                i++;
                sdirconfig = args[i];
            } else if (args[i].equalsIgnoreCase("-limit")) {
                i++;
                slimitconfig = args[i];
            } else if (args[i].equalsIgnoreCase("-auth")) {
                i++;
                shostauth = args[i];
            }
        }
        return true;
    }

    /**
     * @param args
     *            as config_database file [rules_directory host_authent limit_configuration]
     */
    public static void main(String[] args) {
        WaarpLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
        if (logger == null) {
            logger = WaarpLoggerFactory.getLogger(ServerInitDatabase.class);
        }
        if (!getParams(args)) {
            logger.error(_INFO_ARGS);
            if (DbConstant.admin != null && DbConstant.admin.isActive()) {
                DbConstant.admin.close();
            }
            ChannelUtils.stopLogger();
            System.exit(1);
        }

        try {
            if (!FileBasedConfiguration
                    .setConfigurationInitDatabase(Configuration.configuration, args[0], database)) {
                logger
                        .error(Messages.getString("Configuration.NeedCorrectConfig")); //$NON-NLS-1$
                if (DbConstant.admin != null) {
                    DbConstant.admin.close();
                }
                ChannelUtils.stopLogger();
                System.exit(1);
                return;
            }
            if (database) {
                // Init database
                try {
                    initdb();
                } catch (WaarpDatabaseNoConnectionException e) {
                    logger.error(Messages.getString("ServerInitDatabase.ErrDatabase")); //$NON-NLS-1$
                    return;
                }
                System.out.println(Messages.getString("ServerInitDatabase.EndCreation")); //$NON-NLS-1$
            }
            if (upgradeDb) {
                // try to upgrade DB schema
                upgradedb();
                System.out.println(Messages.getString("ServerInitDatabase.EndUpgrade")); //$NON-NLS-1$
            }
            if (sdirconfig != null) {
                // load Rules
                File dirConfig = new File(sdirconfig);
                if (dirConfig.isDirectory()) {
                    loadRules(dirConfig);
                } else {
                    System.err.println(Messages.getString("ServerInitDatabase.NotDirectory") + sdirconfig); //$NON-NLS-1$
                }
            }
            if (shostauth != null) {
                // Load Host Authentications
                if (args.length > 2) {
                    loadHostAuth(shostauth);
                }
            }
            if (slimitconfig != null) {
                // Load configuration
                if (args.length > 2) {
                    FileBasedConfiguration.setConfigurationLoadLimitFromXml(
                            Configuration.configuration,
                            slimitconfig);
                }
            }

            if (sbusiness != null || salias != null || sroles != null) {
                if (sbusiness != null) {
                    File file = new File(sbusiness);
                    if (file.canRead()) {
                        try {
                            String value = FileUtils.readFileToString(file);
                            if (value != null && !value.trim().isEmpty()) {
                                value = value.trim().replaceAll("\r|\n|  ", " ").trim();
                                value = value.replaceAll("\r|\n|  ", " ");
                                sbusiness = value.trim();
                            } else {
                                sbusiness = null;
                            }
                        } catch (IOException e) {
                            sbusiness = null;
                            e.printStackTrace();
                        }
                    } else {
                        sbusiness = null;
                    }
                }
                if (salias != null) {
                    File file = new File(salias);
                    if (file.canRead()) {
                        try {
                            String value = FileUtils.readFileToString(file);
                            if (value != null && !value.trim().isEmpty()) {
                                value = value.trim().replaceAll("\r|\t|\n|  ", " ").trim();
                                value = value.replaceAll("\r|\n|  ", " ");
                                salias = value.trim();
                            } else {
                                salias = null;
                            }
                        } catch (IOException e) {
                            salias = null;
                            e.printStackTrace();
                        }
                    } else {
                        salias = null;
                    }
                }
                if (sroles != null) {
                    File file = new File(sroles);
                    if (file.canRead()) {
                        try {
                            String value = FileUtils.readFileToString(file);
                            if (value != null && !value.trim().isEmpty()) {
                                value = value.trim().replaceAll("\r|\n|  ", " ").trim();
                                value = value.replaceAll("\r|\n|  ", " ");
                                sroles = value.trim();
                            } else {
                                sroles = null;
                            }
                        } catch (IOException e) {
                            sroles = null;
                            e.printStackTrace();
                        }
                    } else {
                        sroles = null;
                    }
                }
                DbHostConfiguration hostConfiguration = null;
                try {
                    hostConfiguration = new DbHostConfiguration(
                            Configuration.configuration.getHOST_ID());
                    if (salias != null) {
                        hostConfiguration.setAliases(salias);
                    }
                    if (sbusiness != null) {
                        hostConfiguration.setBusiness(sbusiness);
                    }
                    if (sroles != null) {
                        hostConfiguration.setRoles(sroles);
                    }
                    hostConfiguration.update();
                } catch (WaarpDatabaseException e) {
                    hostConfiguration = new DbHostConfiguration(
                            Configuration.configuration.getHOST_ID(),
                            sbusiness, sroles, salias, "");
                    try {
                        hostConfiguration.insert();
                    } catch (WaarpDatabaseException e1) {
                        e1.printStackTrace();
                    }
                }
            }
            System.out.println(Messages.getString("ServerInitDatabase.LoadDone")); //$NON-NLS-1$
        } finally {
            if (DbConstant.admin != null) {
                DbConstant.admin.close();
            }
            System.exit(0);
        }
    }

    public static void initdb() throws WaarpDatabaseNoConnectionException {
        // Create tables: configuration, hosts, rules, runner, cptrunner
        DbConstant.admin.getSession().getAdmin().getDbModel().createTables(DbConstant.admin.getSession());
    }

    /**
     *
     * @return True if the base is up to date, else False (need Upgrade)
     */
    public static boolean upgradedb() {
        if (logger == null) {
            logger = WaarpLoggerFactory.getLogger(ServerInitDatabase.class);
        }
        // Update tables: runner
        boolean uptodate = true;
        // ConsistencyCheck if the database is up to date
        String version = DbHostConfiguration
                .getVersionDb(Configuration.configuration.getHOST_ID());
        try {
            if (version != null) {
                uptodate = DbConstant.admin.getSession().getAdmin().getDbModel().needUpgradeDb(DbConstant.admin.getSession(), version, true);
            } else {
                uptodate = DbConstant.admin.getSession().getAdmin().getDbModel().needUpgradeDb(DbConstant.admin.getSession(), "1.1.0", true);
            }
            if (uptodate) {
                logger.error(Messages.getString("ServerInitDatabase.SchemaNotUptodate")); //$NON-NLS-1$
                return false;
            } else {
                logger.debug(Messages.getString("ServerInitDatabase.SchemaUptodate")); //$NON-NLS-1$
            }
        } catch (WaarpDatabaseNoConnectionException e) {
            logger.error(Messages.getString("Database.CannotConnect"), e); //$NON-NLS-1$
            return false;
        }
        return !uptodate;
    }

    public static void loadRules(File dirConfig) {
        try {
            RuleFileBasedConfiguration.importRules(dirConfig);
        } catch (OpenR66ProtocolSystemException e3) {
            e3.printStackTrace();
        } catch (WaarpDatabaseException e) {
            e.printStackTrace();
        }
    }

    public static void loadHostAuth(String filename) {
        AuthenticationFileBasedConfiguration.loadAuthentication(Configuration.configuration,
                filename);
    }
}
