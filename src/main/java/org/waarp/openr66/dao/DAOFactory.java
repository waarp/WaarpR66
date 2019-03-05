package org.waarp.openr66.dao;

import java.sql.Connection;

import org.waarp.common.database.ConnectionFactory;
import org.waarp.openr66.dao.database.DBDAOFactory;
import org.waarp.openr66.dao.exception.DAOException;


/**
 * Abstract class to create DAOFactory
 */
public abstract class DAOFactory {

    private static DAOFactory instance;

    public static void initialize() {
        if (instance != null) {

        }
    }

    public static void initialize(ConnectionFactory factory) {
        if (instance != null) {
            instance = new DBDAOFactory(factory);
        }
    }

    public static DAOFactory getInstance() {
        return instance;
    }

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
     * Return a file based DAOFactory
     *
     * @return a file based factory
     */
    public static DAOFactory getDAOFactory() {
        //return new NoDBDAOFactory();
        return null;
    }

    /**
     * Return a DAOFactory using the Connection provided
     *
     * @param connection the connection used to access the database
     * @return a database DAOFactory using the connection passed as argument;
     * a file based DAO if the Connection is null.
     */
    public static DAOFactory getDAOFactory(Connection connection) {
        if(connection == null) {
            //return new NoDBDAOFactory();
        }
        return new DBDAOFactory(connection);
    }

    /**
     * Return a DAOFactory using the ConnectionFactory provided
     *
     * @param factory the connectionFactory used to access the
     * database
     * @return a database DAOFactory using the connectionFactory passed as
     * argument;
     * a file based DAO if the Connection is null.
     */
    public static DAOFactory getDAOFactory(ConnectionFactory factory) {
        if(factory == null) {
            //return new NoDBDAOFactory();
        }
        return new DBDAOFactory(factory);
    }
}
