/**
   This file is part of GoldenGate Project (named also GoldenGate or GG).

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All GoldenGate Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   GoldenGate is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GoldenGate .  If not, see <http://www.gnu.org/licenses/>.
 */
package openr66.database;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.database.model.DbModelFactory;

/**
 * Class to handle PrepareStatement
 *
 * @author Frederic Bregier LGPL
 *
 */
public class DbPreparedStatement {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(DbPreparedStatement.class);

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
    private final DbSession ls;

    /**
     * Create a DbPreparedStatement from DbSession object
     *
     * @param ls
     * @throws OpenR66DatabaseNoConnectionError
     */
    public DbPreparedStatement(DbSession ls)
            throws OpenR66DatabaseNoConnectionError {
        if (ls == null) {
            logger.error("SQL Exception PreparedStatement no session");
            throw new OpenR66DatabaseNoConnectionError(
                    "PreparedStatement no session");
        }
        DbModelFactory.dbModel.validConnection(ls);
        this.ls = ls;
        rs = null;
        preparedStatement = null;
        isReady = false;
    }

    /**
     * Create a DbPreparedStatement from DbSession object and a request
     *
     * @param ls
     * @param request
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public DbPreparedStatement(DbSession ls, String request)
            throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        if (ls == null) {
            logger.error("SQL Exception PreparedStatement no session");
            throw new OpenR66DatabaseNoConnectionError(
                    "PreparedStatement no session");
        }
        DbModelFactory.dbModel.validConnection(ls);
        this.ls = ls;
        rs = null;
        isReady = false;
        preparedStatement = null;
        if (request == null) {
            logger.error("SQL Exception PreparedStatement no request");
            throw new OpenR66DatabaseNoConnectionError(
                    "PreparedStatement no request");
        }
        try {
            preparedStatement = this.ls.conn.prepareStatement(request);
            this.request = request;
            isReady = true;
        } catch (SQLException e) {
            logger.error("SQL Exception PreparedStatement: " + request+"\n"+ e.getMessage());
            DbSession.error(e);
            preparedStatement = null;
            isReady = false;
            throw new OpenR66DatabaseSqlError(
                    "SQL Exception PreparedStatement", e);
        }
    }

    /**
     * Create a preparedStatement from request
     *
     * @param requestarg
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public void createPrepareStatement(String requestarg)
            throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        if (requestarg == null) {
            logger.error("createPreparedStatement no request");
            throw new OpenR66DatabaseNoConnectionError(
                    "PreparedStatement no request");
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
            logger.error("SQL Exception createPreparedStatement:" + requestarg+
                    "\n"+e.getMessage());
            DbSession.error(e);
            realClose();
            preparedStatement = null;
            isReady = false;
            throw new OpenR66DatabaseSqlError(
                    "SQL Exception createPreparedStatement: " + requestarg, e);
        }
    }

    /**
     * Execute a Select preparedStatement
     *
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     *
     */
    public void executeQuery() throws OpenR66DatabaseNoConnectionError,
            OpenR66DatabaseSqlError {
        if (preparedStatement == null) {
            logger.error("executeQuery no request");
            throw new OpenR66DatabaseNoConnectionError(
                    "executeQuery no request");
        }
        if (rs != null) {
            close();
        }
        try {
            rs = preparedStatement.executeQuery();
        } catch (SQLException e) {
            logger.error("SQL Exception executeQuery:" + request+
                    "\n"+e.getMessage());
            DbSession.error(e);
            close();
            rs = null;
            this.realClose();
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
    public int executeUpdate() throws OpenR66DatabaseNoConnectionError,
            OpenR66DatabaseSqlError {
        if (preparedStatement == null) {
            logger.error("executeUpdate no request");
            throw new OpenR66DatabaseNoConnectionError(
                    "executeUpdate no request");
        }
        if (rs != null) {
            close();
        }
        int retour = -1;
        try {
            retour = preparedStatement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQL Exception executeUpdate:" + request+
                    "\n"+e.getMessage());
            DbSession.error(e);
            this.realClose();
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
    public boolean getNext() throws OpenR66DatabaseNoConnectionError,
            OpenR66DatabaseSqlError {
        if (rs == null) {
            logger.error("SQL ResultSet is Null into getNext");
            throw new OpenR66DatabaseNoConnectionError(
                    "SQL ResultSet is Null into getNext");
        }
        try {
            return rs.next();
        } catch (SQLException e) {
            logger.error("SQL Exception to getNextRow"+
                    "\n"+e.getMessage());
            DbSession.error(e);
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
            throw new OpenR66DatabaseNoConnectionError(
                    "SQL ResultSet is Null into getResultSet");
        }
        return rs;
    }

    /**
     *
     * @return The preparedStatement (should be used in conjunction of
     *         createPreparedStatement)
     * @throws OpenR66DatabaseNoConnectionError
     */
    public PreparedStatement getPreparedStatement()
            throws OpenR66DatabaseNoConnectionError {
        if (preparedStatement == null) {
            throw new OpenR66DatabaseNoConnectionError(
                    "SQL PreparedStatement is Null into getPreparedStatement");
        }
        return preparedStatement;
    }

    /**
     * @return the dbSession
     */
    public DbSession getDbSession() {
        return ls;
    }

}
