package org.waarp.openr66.dao.database;

import java.sql.SQLException;

import org.waarp.common.database.ConnectionFactory;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.openr66.dao.DAOFactory;
import org.waarp.openr66.dao.exception.DAOException;

/**
 * DAOFactory for standard SQL databases
 */
public class DBDAOFactory extends DAOFactory {

    private static WaarpLogger logger = WaarpLoggerFactory.getLogger(DBDAOFactory.class);

    private ConnectionFactory connectionFactory;

    public DBDAOFactory(ConnectionFactory factory) { 
        this.connectionFactory = factory;
    }

    @Override
    public DBBusinessDAO getBusinessDAO() throws DAOException {
        try {
            return new DBBusinessDAO(connectionFactory.getConnection());
        } catch (SQLException e) {
            throw new DAOException("data access error", e);
        }
    }

    @Override
    public DBHostDAO getHostDAO() throws DAOException {
        try {
            return new DBHostDAO(connectionFactory.getConnection());
        } catch (SQLException e) {
            throw new DAOException("data access error", e);
        }
    }

    @Override
    public DBLimitDAO getLimitDAO() throws DAOException {
        try {
            return new DBLimitDAO(connectionFactory.getConnection());
        } catch (SQLException e) {
            throw new DAOException("data access error", e);
        }
    }

    @Override
    public DBMultipleMonitorDAO getMultipleMonitorDAO() throws DAOException {
        try {
            return new DBMultipleMonitorDAO(connectionFactory.getConnection());
        } catch (SQLException e) {
            throw new DAOException("data access error", e);
        }
    }

    @Override
    public DBRuleDAO getRuleDAO() throws DAOException {
        try {
            return new DBRuleDAO(connectionFactory.getConnection());
        } catch (SQLException e) {
            throw new DAOException("data access error", e);
        }
    }

    @Override
    public DBTransferDAO getTransferDAO() throws DAOException {
        try {
             return new DBTransferDAO(connectionFactory.getConnection());
        } catch (SQLException e) {
            throw new DAOException("data access error", e);
        }
    }

    /**
     * Close the DBDAOFactory and close the ConnectionFactory
     * Warning: You need to close the Connection yourself!
     */
    public void close() {
        logger.debug("Closing DAOFactory.");
        logger.debug("Closing factory ConnectionFactory.");
        connectionFactory.close();
    }
}

