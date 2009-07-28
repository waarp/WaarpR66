/**
 *
 */
package openr66.database;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Savepoint;

import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.protocol.utils.OpenR66SignalHandler;

// Notice, do not import com.mysql.jdbc.*
// or you will have problems!

/**
 * Class to handle session with the SGBD of R66
 *
 * @author Frederic Bregier LGPL
 *
 */
public class DbSession {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(DbSession.class);

    /**
     * The internal connection
     */
    public Connection conn = null;

    /**
     * Is this connection Read Only
     */
    public boolean isReadOnly = true;

    /**
     * Create a session and connect the current object to the connect object
     * given as parameter.
     *
     * The database access use no auto commit.
     *
     * If the initialize is not call before, call it with the default value.
     *
     * @param connext
     * @param isReadOnly
     * @throws OpenR66DatabaseNoConnectionError
     */
    public DbSession(Connection connext, boolean isReadOnly) throws OpenR66DatabaseNoConnectionError {
        if (connext == null) {
            logger.error("Cannot set a null connection");
            throw new OpenR66DatabaseNoConnectionError("Cannot set a null Connection");
        }
        conn = connext;
        try {
            conn.setAutoCommit(true);
            this.isReadOnly = isReadOnly;
            conn.setReadOnly(this.isReadOnly);
        } catch (SQLException ex) {
            // handle any errors
            logger.error("Cannot set properties on connection!");
            error(ex);
            conn = null;
            throw new OpenR66DatabaseNoConnectionError("Cannot set properties on connection",ex);
        }
    }

    /**
     * Create a session and connect the current object to the server using the
     * string with the form for mysql for instance
     * jdbc:type://[host:port],[failoverhost:port]
     * .../[database][?propertyName1][
     * =propertyValue1][&propertyName2][=propertyValue2]...
     *
     * By default (if server = null) :
     * "jdbc:mysql://localhost/r66 user=r66 password=r66"
     *
     * The database access use no auto commit.
     *
     * If the initialize is not call before, call it with the default value.
     *
     * @param server
     * @param user
     * @param passwd
     * @param isReadOnly
     * @throws OpenR66DatabaseSqlError
     */
    public DbSession(String server, String user, String passwd,
            boolean isReadOnly) throws OpenR66DatabaseNoConnectionError {
        if (!DbAdmin.classLoaded) {
            DbAdmin.initialize(null);
        }
        if (server == null) {
            conn = null;
            logger.error("Cannot set a null Server");
            throw new OpenR66DatabaseNoConnectionError("Cannot set a null Server");
        }
        try {
            conn = DriverManager.getConnection(server, user, passwd);
            conn.setAutoCommit(true);
            this.isReadOnly = isReadOnly;
            conn.setReadOnly(this.isReadOnly);
            OpenR66SignalHandler.addConnection(conn);
        } catch (SQLException ex) {
            // handle any errors
            logger.error("Cannot create Connection");
            error(ex);
            conn = null;
            throw new OpenR66DatabaseNoConnectionError("Cannot create Connection", ex);
        }
    }

    /**
     * Print the error from SQLException
     *
     * @param ex
     */
    public static void error(SQLException ex) {
        // handle any errors
        logger.error("SQLException: " + ex.getMessage());
        logger.error("SQLState: " + ex.getSQLState());
        logger.error("VendorError: " + ex.getErrorCode());
    }

    /**
     * Close the connection
     * @throws OpenR66DatabaseSqlError
     *
     */
    public void disconnect() throws OpenR66DatabaseSqlError {
        if (conn == null) {
            logger.warn("Connection already closed");
            return;
        }
        try {
            conn.close();
        } catch (SQLException e) {
            logger.warn("Disconnection not OK");
            error(e);
            throw new OpenR66DatabaseSqlError("Cannot disconnect",e);
        }
        OpenR66SignalHandler.removeConnection(conn);
    }

    /**
     * Commit everything
     *
     * @throws OpenR66DatabaseSqlError
     * @throws OpenR66DatabaseNoConnectionError
     */
    public void commit() throws OpenR66DatabaseSqlError, OpenR66DatabaseNoConnectionError {
        if (conn == null) {
            logger.warn("Cannot commit since connection is null");
            throw new OpenR66DatabaseNoConnectionError("Cannot commit since connection is null");
        }
        try {
            conn.commit();
        } catch (SQLException e) {
            logger.error("Cannot Commit");
            error(e);
            throw new OpenR66DatabaseSqlError("Cannot commit",e);
        }
    }

    /**
     * Rollback from the savepoint or the last set if null
     *
     * @param savepoint
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public void rollback(Savepoint savepoint) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        if (conn == null) {
            logger.warn("Cannot rollback since connection is null");
            throw new OpenR66DatabaseNoConnectionError("Cannot rollback since connection is null");
        }
        try {
            if (savepoint == null) {
                conn.rollback();
            } else {
                conn.rollback(savepoint);
            }
        } catch (SQLException e) {
            logger.error("Cannot rollback");
            error(e);
            throw new OpenR66DatabaseSqlError("Cannot rollback", e);
        }
    }

    /**
     * Make a savepoint
     *
     * @return the new savepoint
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public Savepoint savepoint() throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        if (conn == null) {
            logger.warn("Cannot savepoint since connection is null");
            throw new OpenR66DatabaseNoConnectionError("Cannot savepoint since connection is null");
        }
        try {
            return conn.setSavepoint();
        } catch (SQLException e) {
            logger.error("Cannot savepoint");
            error(e);
            throw new OpenR66DatabaseSqlError("Cannot savepoint", e);
        }
    }

    /**
     * Release the savepoint
     *
     * @param savepoint
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public void releaseSavepoint(Savepoint savepoint) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        if (conn == null) {
            logger.warn("Cannot release savepoint since connection is null");
            throw new OpenR66DatabaseNoConnectionError("Cannot release savepoint since connection is null");
        }
        try {
            conn.releaseSavepoint(savepoint);
        } catch (SQLException e) {
            logger.error("Cannot release savepoint");
            error(e);
            throw new OpenR66DatabaseSqlError("Cannot release savepoint", e);
        }
    }
}
