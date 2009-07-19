/**
 *
 */
package openr66.database;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;

/**
 * Class to handle PrepareStatement
 *
 * @author Frederic Bregier LGPL
 *
 */
public class R66DbPreparedStatement {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(R66DbPreparedStatement.class);

    /**
     * Internal PreparedStatement
     */
    private PreparedStatement preparedStatement = null;

    /**
     * The Associated request
     */
    private String request = null;

    /**
     * Is this PreparedStatement ready
     */
    public boolean isReady = false;

    /**
     * The associated resultSet
     */
    private ResultSet rs = null;

    /**
     * The associated DB session
     */
    private R66DbSession ls = null;

    /**
     * Create a R66DbPreparedStatement from R66DbSession object
     *
     * @param ls
     * @throws OpenR66DatabaseNoConnectionError
     */
    public R66DbPreparedStatement(R66DbSession ls) throws OpenR66DatabaseNoConnectionError {
        if (ls == null) {
            logger.error("SQL Exception PreparedStatement no session");
            throw new OpenR66DatabaseNoConnectionError("PreparedStatement no session");
        }
        this.ls = ls;
        rs = null;
        preparedStatement = null;
        isReady = false;
    }

    /**
     * Create a R66DbPreparedStatement from R66DbSession object and a request
     *
     * @param ls
     * @param request
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public R66DbPreparedStatement(R66DbSession ls, String request) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        if (ls == null) {
            logger.error("SQL Exception PreparedStatement no session");
            throw new OpenR66DatabaseNoConnectionError("PreparedStatement no session");
        }
        this.ls = ls;
        rs = null;
        isReady = false;
        preparedStatement = null;
        if (request == null) {
            logger.error("SQL Exception PreparedStatement no request");
            throw new OpenR66DatabaseNoConnectionError("PreparedStatement no request");
        }
        try {
            preparedStatement = this.ls.conn.prepareStatement(request);
            this.request = request;
            isReady = true;
        } catch (SQLException e) {
            logger.error("SQL Exception PreparedStatement: " +
                    request, e);
            preparedStatement = null;
            isReady = false;
            throw new OpenR66DatabaseSqlError("SQL Exception PreparedStatement", e);
        }
    }

    /**
     * Create a preparedStatement from request
     *
     * @param requestarg
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public void createPrepareStatement(String requestarg) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        if (requestarg == null) {
            logger.error("createPreparedStatement no request");
            throw new OpenR66DatabaseNoConnectionError("PreparedStatement no request");
        }
        if (preparedStatement != null) {
            realClose();
        }
        if (rs != null) {
            close();
        }
        try {
            preparedStatement = ls.conn.prepareStatement(requestarg);
            request = requestarg;
            isReady = true;
        } catch (SQLException e) {
            logger.error("SQL Exception createPreparedStatement:" +
                    requestarg, e);
            preparedStatement = null;
            isReady = false;
            throw new OpenR66DatabaseSqlError("SQL Exception createPreparedStatement: " +
                    requestarg, e);
        }
    }

    /**
     * Execute a Select preparedStatement
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     *
     */
    public void executeQuery() throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        if (preparedStatement == null) {
            logger.error("executeQuery no request");
            throw new OpenR66DatabaseNoConnectionError("executeQuery no request");
        }
        if (rs != null) {
            close();
        }
        try {
            rs = preparedStatement.executeQuery();
        } catch (SQLException e) {
            logger.error("SQL Exception executeQuery:" +
                    request, e);
            rs = null;
            throw new OpenR66DatabaseSqlError("SQL Exception executeQuery: " +
                    request, e);
        }
    }

    /**
     * Execute the Update/Insert/Delete preparedStatement
     *
     * @return the number of row
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public int executeUpdate() throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        if (preparedStatement == null) {
            logger.error("executeUpdate no request");
            throw new OpenR66DatabaseNoConnectionError("executeUpdate no request");
        }
        if (rs != null) {
            close();
        }
        int retour = -1;
        try {
            retour = preparedStatement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQL Exception executeUpdate:" +
                    request, e);
            throw new OpenR66DatabaseSqlError("SQL Exception executeUpdate: " +
                    request, e);
        }
        return retour;
    }

    /**
     * Close the resultSet if any
     *
     */
    public void close() {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
            }
            rs = null;
        }
    }

    /**
     * Really close the preparedStatement and the resultSet if any
     *
     */
    public void realClose() {
        close();
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
            }
            preparedStatement = null;
        }
        isReady = false;
    }

    /**
     * Move the cursor to the next result
     *
     * @return True if there is a next result, else False
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public boolean getNext() throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        if (rs == null) {
            logger.error("SQL ResultSet is Null into getNext");
            throw new OpenR66DatabaseNoConnectionError("SQL ResultSet is Null into getNext");
        }
        try {
            return rs.next();
        } catch (SQLException e) {
            logger.error("SQL Exception to getNextRow", e);
            throw new OpenR66DatabaseSqlError("SQL Exception to getNextRow: " +
                    request, e);
        }
    }

    /**
    *
    * @return The resultSet (can be used in conjunction of getNext())
    * @throws OpenR66DatabaseNoConnectionError
    */
   public ResultSet getResultSet() throws OpenR66DatabaseNoConnectionError {
       if (rs == null) {
           throw new OpenR66DatabaseNoConnectionError("SQL ResultSet is Null into getResultSet");
       }
       return rs;
   }

   /**
    *
    * @return The preparedStatement (should be used in conjunction of createPreparedStatement)
    * @throws OpenR66DatabaseNoConnectionError
    */
   public PreparedStatement getPreparedStatement() throws OpenR66DatabaseNoConnectionError {
       if (preparedStatement == null) {
           throw new OpenR66DatabaseNoConnectionError("SQL PreparedStatement is Null into getPreparedStatement");
       }
       return preparedStatement;
   }
}
