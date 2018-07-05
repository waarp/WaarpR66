package org.waarp.openr66.dao.database;

import java.sql.Connection;
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
    private Connection sharedConnection;

    public DBDAOFactory(Connection con) { 
        this.sharedConnection = con;
        this.connectionFactory = null;
    }

    public DBDAOFactory(ConnectionFactory factory) { 
        this.sharedConnection = null;
        this.connectionFactory = factory;
    }

    private Connection getConnection() throws DAOException {
        if (connectionFactory != null) {
            try {
                return connectionFactory.getConnection();
            } catch (SQLException e) {
                throw new DAOException("Data access Error.", e);
            }
        }
        return sharedConnection;
    }

    @Override
    public DBBusinessDAO getBusinessDAO() throws DAOException {
        return new DBBusinessDAO(getConnection());
    }

    @Override
    public DBHostDAO getHostDAO() throws DAOException {
        return new DBHostDAO(getConnection());
    }

    @Override
    public DBLimitDAO getLimitDAO() throws DAOException {
        return new DBLimitDAO(getConnection());
    }

    @Override
    public DBMultipleMonitorDAO getMultipleMonitorDAO() throws DAOException {
        return new DBMultipleMonitorDAO(getConnection());
    }

    @Override
    public DBRuleDAO getRuleDAO() throws DAOException {
        return new DBRuleDAO(getConnection());
    }

    @Override
    public DBTransferDAO getTransferDAO() throws DAOException {
        return new DBTransferDAO(getConnection());
    }

    /**
     * Close the DBDAOFactory and close the ConnectionFactory if one was
     * provided.
     * Warning: You need to close the Connection yourself!
     */
    public void close() {
        logger.debug("Closing DAOFactory.");
        if (sharedConnection != null) {
            logger.debug("Closing factory Connection.");
            try {
                sharedConnection.close();
            } catch (SQLException e) {
                logger.warn("An error occurs while atempting to close the Database connection.", e);
            }
        } else {
            logger.debug("Closing factory ConnectionFactory.");
            connectionFactory.close();
        }
    }
}

