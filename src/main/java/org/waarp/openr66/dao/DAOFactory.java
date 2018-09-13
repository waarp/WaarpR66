package org.waarp.openr66.dao;

import java.sql.SQLException;

import org.waarp.common.database.ConnectionFactory;
import org.waarp.openr66.dao.database.DBDAOFactory;
import org.waarp.openr66.dao.exception.DAOException;


/**
 * Abstract class to create DAOFactory
 */
public abstract class DAOFactory {

    /**
     * Return a BusinessDAO
     *
     * @return a ready to use BusinessDAO
     * @throws DAOException if cannot create the DAO
     */
    public abstract BusinessDAO getBusinessDAO() throws DAOException;

    /**
     * Return a HostDAO
     *
     * @return a ready to use HostDAO
     * @throws DAOException if cannot create the DAO
     */
    public abstract HostDAO getHostDAO() throws DAOException;

    /**
     * Return a LimitDAO
     *
     * @return a ready to use LimitDAO
     * @throws DAOException if cannot create the DAO
     */
    public abstract LimitDAO getLimitDAO() throws DAOException;

    /**
     * Return a MultipleMonitorDAO
     *
     * @return a ready to use MultipleMonitorDAO
     * @throws DAOException if cannot create the DAO
     */
    public abstract MultipleMonitorDAO getMultipleMonitorDAO() 
            throws DAOException;

    /**
     * Return a RuleDAO
     *
     * @return a ready to use RuleDAO
     * @throws DAOException if cannot create the DAO
     */
    public abstract RuleDAO getRuleDAO() throws DAOException;

    /**
     * Return a TransferDAO
     *
     * @return a ready to use TramsferDAO
     * @throws DAOException if cannot create the DAO
     */
    public abstract TransferDAO getTransferDAO() throws DAOException;

    /**
     * 
     */
    public abstract void close();

    /**
     * 
     */
    public static DAOFactory getDAOFactory() throws DAOException {
        ConnectionFactory factory = ConnectionFactory.getInstance();
        if (factory == null) {
            //return new noDBDAOFactory
            return null;
        } else {
            try {
                return new DBDAOFactory(factory.getConnection());
            } catch (SQLException e) {
                throw new DAOException(e);
            }
        }
    }

    /**
     *
     */
    public static DAOFactory getDAOFactory(boolean readOnly) throws DAOException {
        ConnectionFactory factory = ConnectionFactory.getInstance();
        if (factory == null) {
            //return new noDBDAOFactory
            return null;
        } else {
            try {
                return new DBDAOFactory(factory.getConnection(readOnly));
            } catch (SQLException e) {
                throw new DAOException(e);
            }
        }
    }

    /**
     *
     */
    public static DAOFactory getDAOFactory(boolean readOnly,
            boolean autoCommit) throws DAOException {
        ConnectionFactory factory = ConnectionFactory.getInstance();
        if (factory == null) {
            //return new noDBDAOFactory
            return null;
        } else {
            try {
                return new DBDAOFactory(factory.getConnection(
                            readOnly, autoCommit));
            } catch (SQLException e) {
                throw new DAOException(e);
            }
        }
    }
}
