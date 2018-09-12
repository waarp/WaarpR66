package org.waarp.openr66.configuration;

import java.sql.SQLException;

import org.waarp.common.database.ConnectionFactory;
import org.waarp.common.xml.XmlHash;
import org.waarp.common.xml.XmlValue;

public class DatabaseConfiguration {

    private static final String XML_URL = "dbserver"; 
    private static final String XML_USER = "dbuser"; 
    private static final String XML_PASSWORD = "dbpassword"; 
    private static final String XML_READONLY = "dbreadonly"; 
    private static final String XML_AUTOCOMMIT = "dbautocommit"; 
    private static final String XML_MAX_CONNECTION = "dbmaxconnection"; 
    private static final String XML_CONNECTION_TIMEOUT = "dbconnectiontimeout"; 
    private static final String XML_VALIDATION_TIMEOUT = "dbvalidationtimeout"; 

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

    /**
     * ConnectionFactory to ask connection to the database
     */
    public static ConnectionFactory factory = null;

    public static void readFromXML(XmlHash xml) {
        //Read url
        XmlValue value = xml.get(XML_URL);
        if (value == null || (value.isEmpty())) { 
            //throw BadConfigurationException("Bad Database Configuration");
            return;
        }
        url = value.getString();
        //Read user
        value = xml.get(XML_USER);
        if (value == null || (value.isEmpty())) { 
            //throw BadConfigurationException("Bad Database Configuration");
            return;
        }
        user = value.getString();        
        //Read password
        value = xml.get(XML_USER);
        if (value == null || (value.isEmpty())) { 
            //throw BadConfigurationException("Bad Database Configuration");
            return;
        }
        password = value.getString();        
        //Read readOnly
        value = xml.get(XML_READONLY);
        if (value != null && !(value.isEmpty())) { 
            defaultReadOnly = value.getBoolean();        
        }
        //Read autoCommit
        value = xml.get(XML_AUTOCOMMIT);
        if (value != null && !(value.isEmpty())) { 
            defaultAutoCommit = value.getBoolean();        
        }
        //Read maxConnection
        value = xml.get(XML_MAX_CONNECTION);
        if (value != null && !(value.isEmpty())) { 
            maxConnection = value.getInteger();        
        }
        //Read connectionTimeout
        value = xml.get(XML_CONNECTION_TIMEOUT);
        if (value != null && !(value.isEmpty())) { 
            connectionTimeout = value.getInteger();        
        }
        //Read autoCommit
        value = xml.get(XML_VALIDATION_TIMEOUT);
        if (value != null && !(value.isEmpty())) { 
            validationTimeout = value.getInteger();        
        }
    }

    public static void init() {
        if (url != null) {
            try {
                factory = new ConnectionFactory(
                        ConnectionFactory.propertiesFor(url), url, user, password, 
                        defaultReadOnly, defaultAutoCommit, maxConnection,
                        connectionTimeout, validationTimeout);
            } catch (SQLException e) {
                //throw BadConfigurationException("Bad Database Configuration");
                return;
            }
        } else {
            //throw BadConfigurationException("Bad Database Configuration");
            return;
        }
    }

    public static void close() {
        if (factory != null) {
            factory.close();
        }
    }
}

