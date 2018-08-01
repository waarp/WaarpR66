package org.waarp.openr66.dao.database;

import java.util.ArrayList;
import java.util.List;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.openr66.dao.MultipleMonitorDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.MultipleMonitor;

/**
 * Implementation of MultipleMonitorDAO for a standard SQL database
 */
public class DBMultipleMonitorDAO implements MultipleMonitorDAO {

    private static final WaarpLogger logger = WaarpLoggerFactory.getLogger(DBMultipleMonitorDAO.class);

    protected static String TABLE = "MULTIPLEMONITOR";

    protected static String HOSTID_FIELD = "hostid";
    protected static String COUNT_CONFIG_FIELD = "countconfig";
    protected static String COUNT_HOST_FIELD = "counthost";
    protected static String COUNT_RULE_FIELD = "countrule";

    protected static String deleteQuery = "DELETE FROM " + TABLE + 
        " WHERE " + HOSTID_FIELD + " = ?";
    protected static String insertQuery = "INSERT INTO " + TABLE + 
        " (" +  ALL_FIELD() + ") VALUES (?,?,?,?)";
    protected static String existQuery = "SELECT 1 FROM " + TABLE + 
        " WHERE " + HOSTID_FIELD + " = ?";
    protected static String selectQuery = "SELECT * FROM " + TABLE + 
        " WHERE " + HOSTID_FIELD + " = ?";
    protected static String updateQuery = "UPDATE " + TABLE + 
        " SET " + SET_ALL_FIELD() + " WHERE " + HOSTID_FIELD + " = ?";

    protected static String ALL_FIELD() {
        return HOSTID_FIELD + ", " +
            COUNT_CONFIG_FIELD + ", " + 
            COUNT_HOST_FIELD + ", " + 
            COUNT_RULE_FIELD; 
    };

    protected static String SET_ALL_FIELD() {
        return HOSTID_FIELD + " = ?, " +
            COUNT_CONFIG_FIELD + " = ?, " + 
            COUNT_HOST_FIELD + " = ?, " + 
            COUNT_RULE_FIELD + " = ?"; 
    };

    protected Connection connection;    
    protected PreparedStatement deleteStatement;
    protected PreparedStatement insertStatement;
    protected PreparedStatement existStatement;
    protected PreparedStatement selectStatement;
    protected PreparedStatement updateStatement;

    public DBMultipleMonitorDAO(Connection con) throws DAOException {
        this.connection = con;
        try {
            this.deleteStatement = con.prepareStatement(deleteQuery);
            this.insertStatement = con.prepareStatement(insertQuery);
            this.existStatement = con.prepareStatement(existQuery);
            this.selectStatement = con.prepareStatement(selectQuery);
            this.updateStatement = con.prepareStatement(updateQuery);
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
    }

    @Override
    public void delete(MultipleMonitor multipleMonitor) throws DAOException {
        int res = 0;
        try {
            deleteStatement.setNString(0, multipleMonitor.getHostid());
            res = deleteStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res == 0) {
            logger.warn("Unable to delete multipleMonitor " + multipleMonitor.getHostid() 
                    + ", entry not found.");
            return;
        }
        logger.info("Successfully deleted multipleMonitor entry " + multipleMonitor.getHostid() + ".");
    }

    @Override
    public void deleteAll() throws DAOException {
        Statement stm = null;
        int res = 0;
        try {
            stm = connection.createStatement();
            res = stm.executeUpdate("DELETE FROM " + TABLE);
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        } finally {
            try {
                if (stm != null) {
                    stm.close();
                }
            } catch (SQLException e) {
                logger.warn("An error occurs while atempting to close the database statement.", e);
            }
        }
        logger.info("Successfully deleted " + res +  " multipleMonitor entries.");
    }

    @Override
    public List<MultipleMonitor> getAll() throws DAOException {
        Statement stm = null;
        ResultSet res = null;
        ArrayList<MultipleMonitor> multipleMonitors = new ArrayList<MultipleMonitor>();
        try {
            stm = connection.createStatement();
            res = stm.executeQuery("SELECT * FROM " + TABLE);
            while (res.next()) {
                multipleMonitors.add(getFromResultSet(res));
            }
            return multipleMonitors;
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        } finally {
            try {
                if (res != null) {
                    res.close();
                }
                if (stm != null) {
                    stm.close();
                }
            } catch (SQLException e) {
                logger.warn("An error occurs while atempting to close the Statement.", e);
            }
        }
    }

    @Override
    public MultipleMonitor get(String hostid) throws DAOException {
        ResultSet res = null;
        try {
            selectStatement.setNString(0, hostid);
            res = selectStatement.executeQuery();
            if (!res.next()) {
                logger.warn("Unable to retrieve multipleMonitor " + hostid 
                        + ", entry not found.");
                return null;
            }
            return getFromResultSet(res);
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        } finally {
            try {
                if (res != null) {
                    res.close();
                }
            } catch (SQLException e) {
                logger.warn("An error occurs while atempting to close the ResultSet.", e);
            }
        }
    }

    @Override
    public void insert(MultipleMonitor multipleMonitor) throws DAOException {
        int res = 0;
        try {
            insertStatement.setNString(0, multipleMonitor.getHostid());
            insertStatement.setInt(1, multipleMonitor.getCountConfig());
            insertStatement.setInt(2, multipleMonitor.getCountHost());
            insertStatement.setInt(3, multipleMonitor.getCountRule());
            res = insertStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res != 1) {
            logger.warn("No entrie inserted for multipleMonitors " + multipleMonitor.getHostid() + ".");
            return;
        } 
        logger.info("Successfully inserted multipleMonitor entry " + multipleMonitor.getHostid() + ".");
    }

    @Override
    public boolean exist(String hostid) throws DAOException {
        ResultSet res = null;
        try {
            existStatement.setNString(0, hostid);
            res = selectStatement.executeQuery();
            return res.isBeforeFirst();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        } finally {
            try {
                if (res != null) {
                    res.close();
                }
            } catch (SQLException e) {
                logger.warn("An error occurs while atempting to close the ResultSet.", e);
            }
        }
    }

    @Override
    public void update(MultipleMonitor multipleMonitor) throws DAOException {
        int res = 0;
        try {
            updateStatement.setNString(0, multipleMonitor.getHostid());
            updateStatement.setInt(1, multipleMonitor.getCountConfig());
            updateStatement.setInt(2, multipleMonitor.getCountHost());
            updateStatement.setInt(3, multipleMonitor.getCountRule());
            updateStatement.setNString(4, multipleMonitor.getHostid());
            res = updateStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res != 1) {
            logger.warn("Unable to update multipleMonitor " + multipleMonitor.getHostid()
                    + ", entry not found.");
            return;
        } 
        logger.info("Successfully updated multipleMonitor entry " + multipleMonitor.getHostid() + ".");
    }

    private MultipleMonitor getFromResultSet(ResultSet set) throws SQLException {
        return new MultipleMonitor(
                set.getNString(HOSTID_FIELD),
                set.getInt(COUNT_CONFIG_FIELD),
                set.getInt(COUNT_HOST_FIELD),
                set.getInt(COUNT_RULE_FIELD));
    }
}

