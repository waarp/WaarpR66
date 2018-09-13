package org.waarp.openr66.configuration;

import java.sql.SQLException;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;

import org.waarp.common.database.ConnectionFactory;

/**
 * Class to read and access the Database Configuration
 */
public class DatabaseConfiguration {

    private static final String PATH_URL = "database.dbserver"; 
    private static final String PATH_USER = "database.dbuser"; 
    private static final String PATH_PASSWORD = "database.dbpassword"; 
    private static final String PATH_READONLY = "database.dbreadonly"; 
    private static final String PATH_AUTOCOMMIT = "database.dbautocommit"; 
    private static final String PATH_MAX_CONNECTION = "database.dbmaxconnection"; 
    private static final String PATH_CONNECTION_TIMEOUT = "datatabase.dbconnectiontimeout"; 
    private static final String PATH_VALIDATION_TIMEOUT = "database.dbvalidationtimeout"; 

    /**
     * Url used to connect to the database.
     */
    public static String url = null;

    /**
     * User used to connect to the database
     */
    public static String user = null;

    /**
     * Password used to connect to the database
     */
    public static String password = null;

    /**
     * The default readOnly status of the connection
     */
    public static boolean defaultReadOnly = true;

    /**
     * The default autoCommit status of the connection
     */
    public static boolean defaultAutoCommit = false;

    /**
     * The maximum size of the connection pool
     */
    public static int maxConnection = 5000;

    /**
     * The maximum waiting time (ms) to get a connection
     */
    public static int connectionTimeout = 30;

    /**
     * The maximum waiting time (ms) to validate a connection
     */
    public static int validationTimeout = 2;

    public static void init(Configuration conf) 
            throws BadConfigurationException {
        // Read Configuration
        try {
            // Mandatory configuration
            url = conf.getString(PATH_URL);
            user = conf.getString(PATH_USER);  
            password = conf.getString(PATH_PASSWORD);
            // Optionial configuration
            defaultReadOnly = conf.getBoolean(PATH_READONLY, defaultReadOnly);
            defaultAutoCommit = conf.getBoolean(PATH_AUTOCOMMIT, 
                    defaultAutoCommit);
            maxConnection = conf.getInt(PATH_MAX_CONNECTION, maxConnection);
            connectionTimeout = conf.getInt(PATH_CONNECTION_TIMEOUT,
                    connectionTimeout);
            validationTimeout = conf.getInt(PATH_VALIDATION_TIMEOUT,
                    validationTimeout);
        } catch (ConfigurationRuntimeException e) {
             throw new BadConfigurationException("Error while parsing configuration", e);
        }
        // Init ConnectionFactory
        try {
            ConnectionFactory.init(ConnectionFactory.propertiesFor(url), 
                    url, user, password, defaultReadOnly, defaultAutoCommit, 
                    maxConnection, connectionTimeout, validationTimeout);
        } catch (SQLException e) {
            throw new BadConfigurationException("Cannot connect to database", e);
        }
    }
}

