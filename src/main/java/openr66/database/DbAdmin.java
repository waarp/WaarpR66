/**
 *
 */
package openr66.database;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.protocol.config.Configuration;

/**
 * Class for Admin through Database
 *
 * @author Frederic Bregier LGPL
 *
 */
public class DbAdmin {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(DbAdmin.class);

    public static enum DatabaseType {
        Oracle, MySQL, PostGreSQL, H2;
    }

    /**
     * Database type
     */
    public DatabaseType typeDriver;

    /**
     * DB Server
     */
    private String server = null;

    /**
     * DB User
     */
    private String user = null;

    /**
     * DB Password
     */
    private String passwd = null;

    /**
     * Is this DB Admin connected
     */
    public boolean isConnected = false;

    /**
     * Is this DB Admin Read Only
     */
    public boolean isReadOnly = false;

    /**
     * Is this DB Admin accessed by only one thread at a time (no concurrency
     * and no lock/unlock problem)
     */
    public boolean isMultipleDBAccess = false;

    /**
     * session is the Session object for all type of requests
     */
    public DbSession session = null;

    /**
     * Info on JDBC Class is already loaded or not
     */
    static public boolean classLoaded = false;

    /**
     * Load the correct jdbc driver (default com.mysql.jdbc.Driver)
     *
     * @param driver
     * @throws OpenR66DatabaseNoConnectionError
     */
    public static void initialize(DatabaseType typeDriver) throws OpenR66DatabaseNoConnectionError {
        if (classLoaded) {
            return;
        }
        try {
            switch (typeDriver) {
                case Oracle:
                    DriverManager
                            .registerDriver(new oracle.jdbc.OracleDriver());
                    break;
                case H2:
                    DriverManager.registerDriver(new org.h2.Driver());
                    break;
                default:
                    logger.error("Cannot load database drive:" +
                            typeDriver.name());
                    throw new OpenR66DatabaseNoConnectionError("Cannot load database drive:" +
                            typeDriver.name());
            }
            classLoaded = true;
        } catch (SQLException e) {
            // SQLException
            logger.error("Cannot register Driver " + typeDriver.name(), e);
            throw new OpenR66DatabaseNoConnectionError("Cannot load database drive:" +
                    typeDriver.name(), e);
        }
    }

    /**
     * Validate connection
     * @throws OpenR66DatabaseNoConnectionError
     */
    public void validConnection() throws OpenR66DatabaseNoConnectionError {
        if (typeDriver == DatabaseType.Oracle ||
                typeDriver == DatabaseType.MySQL) {
            DbRequest request = new DbRequest(session);
            try {
                request.select("select 1 from dual");
                if (!request.getNext()) {
                    isConnected = false;
                    logger.error("Cannot connect to Database!");
                    throw new OpenR66DatabaseNoConnectionError("Cannot connect to database");
                }
            } catch (OpenR66DatabaseSqlError e) {
                isConnected = false;
                logger.error("Cannot connect to Database!");
                throw new OpenR66DatabaseNoConnectionError("Cannot connect to database");
            }
            request.close();
        } else {
            DbRequest request = new DbRequest(session);
            try {
                request.select("select 1");
                if (!request.getNext()) {
                    isConnected = false;
                    logger.error("Cannot connect to Database!");
                    throw new OpenR66DatabaseNoConnectionError("Cannot connect to database");
                }
            } catch (OpenR66DatabaseSqlError e) {
                isConnected = false;
                logger.error("Cannot connect to Database!");
                throw new OpenR66DatabaseNoConnectionError("Cannot connect to database");
            }
            request.close();
        }

    }
    /**
     * Use a default server for basic connection. Later on, specific connection to
     * database for the scheme that provides access to the table R66DbIndex for
     * one specific Legacy could be done.
     *
     * A this time, only one driver is possible! If a new driver is needed, then
     * we need to create a new DbSession object. Be aware that
     * DbSession.initialize should be call only once for each driver,
     * whatever the number of DbSession objects that could be created (=>
     * need a hashtable for specific driver when created). Also, don't know if
     * two drivers at the same time (two different DbSession) is allowed by
     * JDBC.
     *
     * @param driver
     * @param server
     * @param user
     * @param passwd
     * @throws OpenR66DatabaseNoConnectionError
     */
    public DbAdmin(String driver, String server, String user, String passwd) throws OpenR66DatabaseNoConnectionError {
        this.server = server;
        this.user = user;
        this.passwd = passwd;
        if (driver.contains("oracle")) {
            typeDriver = DatabaseType.Oracle;
        } else if (driver.contains("mysql")) {
            typeDriver = DatabaseType.MySQL;
        } else if (driver.contains("postgresql")) {
            typeDriver = DatabaseType.PostGreSQL;
        } else if (driver.contains("h2")) {
            typeDriver = DatabaseType.H2;
        } else {
            logger.error("Cannot find TypeDriver:" + driver);
            throw new OpenR66DatabaseNoConnectionError("Cannot find database drive:" +
                    driver);
        }
        // R66Constants.logger.warn("XXXTYPE:"+R66Constants.typeDriver+":"+driver);
        DbAdmin.initialize(typeDriver);
        // "jdbc:h2:~/r66;IFEXISTS=TRUE;IGNORECASE=TRUE";
        session = new DbSession(this.server, this.user, this.passwd, false);
        isReadOnly = false;
        validConnection();
        isConnected = true;
    }

    /**
     * Use a default server for basic connection. Later on, specific connection to
     * database for the scheme that provides access to the table R66DbIndex for
     * one specific Legacy could be done.
     *
     * A this time, only one driver is possible! If a new driver is needed, then
     * we need to create a new DbSession object. Be aware that
     * DbSession.initialize should be call only once for each driver,
     * whatever the number of DbSession objects that could be created (=>
     * need a hashtable for specific driver when created). Also, don't know if
     * two drivers at the same time (two different DbSession) is allowed by
     * JDBC.
     *
     * @param driver
     * @param server
     * @param user
     * @param passwd
     * @param write
     * @throws OpenR66DatabaseSqlError
     * @throws OpenR66DatabaseNoConnectionError
     */
    public DbAdmin(String driver, String server, String user, String passwd,
            boolean write) throws OpenR66DatabaseNoConnectionError {
        this.server = server;
        this.user = user;
        this.passwd = passwd;
        if (driver.contains("oracle")) {
            typeDriver = DatabaseType.Oracle;
        } else if (driver.contains("mysql")) {
            typeDriver = DatabaseType.MySQL;
        } else if (driver.contains("postgresql")) {
            typeDriver = DatabaseType.PostGreSQL;
        } else if (driver.contains("h2")) {
            typeDriver = DatabaseType.H2;
        } else {
            logger.error("Cannot find TypeDriver:" + driver);
            throw new OpenR66DatabaseNoConnectionError("Cannot find database drive:" +
                    driver);
        }
        DbAdmin.initialize(typeDriver);
        if (write) {
            for (int i = 0; i < Configuration.RETRYNB; i ++) {
                try {
                    session = new DbSession(this.server, this.user, this.passwd,
                            false);
                } catch (OpenR66DatabaseNoConnectionError e) {
                    logger.warn("Attempt of connection in error: "+i);
                    continue;
                }
                isReadOnly = false;
                validConnection();
                return;
            }
        } else {
            for (int i = 0; i < Configuration.RETRYNB; i ++) {
                try {
                    session = new DbSession(this.server, this.user, this.passwd,
                            true);
                } catch (OpenR66DatabaseNoConnectionError e) {
                    logger.warn("Attempt of connection in error: "+i);
                    continue;
                }
                isReadOnly = true;
                validConnection();
                return;
            }
        }
        session = null;
        isConnected = false;
        logger.error("Cannot connect to Database!");
        throw new OpenR66DatabaseNoConnectionError("Cannot connect to database");
    }

    /**
     * Use a default server for basic connection. Later on, specific connection to
     * database for the scheme that provides access to the table R66DbIndex for
     * one specific Legacy could be done.
     *
     * A this time, only one driver is possible! If a new driver is needed, then
     * we need to create a new DbSession object. Be aware that
     * DbSession.initialize should be call only once for each driver,
     * whatever the number of DbSession objects that could be created (=>
     * need a hashtable for specific driver when created). Also, don't know if
     * two drivers at the same time (two different DbSession) is allowed by
     * JDBC.<BR>
     *
     * <B>This version use given connection. typeDriver must be set before !</B>
     *
     * @param conn
     * @param isread
     * @throws OpenR66DatabaseNoConnectionError
     */
    public DbAdmin(Connection conn, boolean isread) throws OpenR66DatabaseNoConnectionError {
        server = null;
        if (conn == null) {
            session = null;
            isConnected = false;
            logger.error("Cannot Get a Connection from Datasource");
            throw new OpenR66DatabaseNoConnectionError("Cannot Get a Connection from Datasource");
        }
        session = new DbSession(conn, isread);
        isReadOnly = isread;
        isConnected = true;
    }

    /**
     * Close the underlying session. Can be call even for connection given from
     * the constructor DbAdmin(Connection, boolean).
     * @throws OpenR66DatabaseSqlError
     *
     */
    public void close() throws OpenR66DatabaseSqlError {
        if (session != null) {
            session.disconnect();
            session = null;
        }
        isConnected = false;
    }

    /**
     * Commit on connection
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     *
     */
    public void commit() throws OpenR66DatabaseSqlError, OpenR66DatabaseNoConnectionError {
        if (session != null) {
            session.commit();
        }
    }
}
