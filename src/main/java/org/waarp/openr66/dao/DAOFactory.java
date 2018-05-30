package org.waarp.openr66.dao;

import java.sql.Connection;

import org.waarp.common.database.ConnectionFactory;
import org.waarp.openr66.dao.database.DBDAOFactory;
import org.waarp.openr66.dao.exception.DAOException;

public abstract class DAOFactory {
    public abstract BusinessDAO getBusinessDAO() throws DAOException;
    public abstract HostDAO getHostDAO() throws DAOException;
    public abstract LimitDAO getLimitDAO() throws DAOException;
    public abstract MultipleMonitorDAO getMultipleMonitorDAO() 
            throws DAOException;
    public abstract RuleDAO getRuleDAO() throws DAOException;
    public abstract TransferDAO getTransferDAO() throws DAOException;

    public static DAOFactory getDAOFactory() {
        //return new NoDBDAOFactory();
        return null;
    }

    public static DAOFactory getDAOFactory(Connection con) {
        if(con == null) {
            //return new NoDBDAOFactory();
        }
        return new DBDAOFactory(con);
    }

    public static DAOFactory getDAOFactory(ConnectionFactory factory) {
        if(factory == null) {
            //return new NoDBDAOFactory();
        }
        return new DBDAOFactory(factory);
    }
}
