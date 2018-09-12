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

    private Connection connection;

    public DBDAOFactory(Connection con) { 
        this.connection = con;
    }

    @Override
    public DBBusinessDAO getBusinessDAO() throws DAOException {
        return new DBBusinessDAO(connection);
    }

    @Override
    public DBHostDAO getHostDAO() throws DAOException {
        return new DBHostDAO(connection);
    }

    @Override
    public DBLimitDAO getLimitDAO() throws DAOException {
        return new DBLimitDAO(connection);
    }

    @Override
    public DBMultipleMonitorDAO getMultipleMonitorDAO() throws DAOException {
        return new DBMultipleMonitorDAO(connection);
    }

    @Override
    public DBRuleDAO getRuleDAO() throws DAOException {
        return new DBRuleDAO(connection);
    }

    @Override
    public DBTransferDAO getTransferDAO() throws DAOException {
        return new DBTransferDAO(connection);
    }

    /**
     *
     */
    public void close() {
        logger.debug("Closing DAOFactory.");
        try {
            connection.close();
        } catch (SQLException e) {
            logger.warn("An error occurs while atempting to close the Database connection.", e);
        }
    }
}

