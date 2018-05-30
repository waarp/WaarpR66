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
import org.waarp.openr66.dao.BusinessDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Business;

public class DBBusinessDAO implements BusinessDAO {

    private static final WaarpLogger logger = WaarpLoggerFactory.getLogger(DBBusinessDAO.class);

    protected static String TABLE = "HOSTCONFIG";

    protected static String HOSTID_FIELD = "hostid";
    protected static String BUSINESS_FIELD = "business";
    protected static String ROLES_FIELD = "roles";
    protected static String ALIASES_FIELD = "aliases";
    protected static String OTHERS_FIELD = "others";

    protected static String deleteQuery = "DELETE FROM " + TABLE + 
        " WHERE " + HOSTID_FIELD + " = ?";
    protected static String insertQuery = "INSERT INTO " + TABLE + 
        " (" +  ALL_FIELD() + ") VALUES (?,?,?,?,?)";
    protected static String existQuery = "SELECT 1 FROM " + TABLE + 
        " WHERE " + HOSTID_FIELD + " = ?";
    protected static String selectQuery = "SELECT * FROM " + TABLE + 
        " WHERE " + HOSTID_FIELD + " = ?";
    protected static String updateQuery = "UPDATE " + TABLE + 
        " SET " + SET_ALL_FIELD() + " WHERE " + HOSTID_FIELD + " = ?";

    protected static String ALL_FIELD() {
        return HOSTID_FIELD + ", " +
            BUSINESS_FIELD + ", " + 
            ROLES_FIELD + ", " + 
            ALIASES_FIELD + ", " + 
            OTHERS_FIELD; 
    };

    protected static String SET_ALL_FIELD() {
        return HOSTID_FIELD + " = ?, " +
            BUSINESS_FIELD + " = ?, " + 
            ROLES_FIELD + " = ?, " + 
            ALIASES_FIELD + " = ?, " + 
            OTHERS_FIELD + " = ?"; 
    };

    protected Connection connection;    
    protected PreparedStatement deleteStatement;
    protected PreparedStatement insertStatement;
    protected PreparedStatement existStatement;
    protected PreparedStatement selectStatement;
    protected PreparedStatement updateStatement;

    public DBBusinessDAO(Connection con) throws DAOException {
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
    public void delete(Business business) throws DAOException {
        int res = 0;
        try {
            deleteStatement.setNString(0, business.getHostid());
            res = deleteStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res == 0) {
            logger.warn("Unable to delete business " + business.getHostid() 
                    + ", entry not found.");
            return;
        }
        logger.info("Successfully deleted business entry " + business.getHostid() + ".");
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
        logger.info("Successfully deleted " + res +  " business entries.");
    }

    @Override
    public List<Business> getAll() throws DAOException {
        Statement stm = null;
        ResultSet res = null;
        ArrayList<Business> businesses = new ArrayList<Business>();
        try {
            stm = connection.createStatement();
            res = stm.executeQuery("SELECT * FROM " + TABLE);
            while (res.next()) {
                businesses.add(getFromResultSet(res));
            }
            return businesses;
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
    public Business get(String hostid) throws DAOException {
        ResultSet res = null;
        try {
            selectStatement.setNString(0, hostid);
            res = selectStatement.executeQuery();
            if (!res.next()) {
                logger.warn("Unable to retrieve business " + hostid 
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
    public void insert(Business business) throws DAOException {
        int res = 0;
        try {
            insertStatement.setNString(0, business.getHostid());
            insertStatement.setNString(1, business.getBusiness());
            insertStatement.setNString(2, business.getAliases());
            insertStatement.setNString(3, business.getRoles());
            insertStatement.setNString(4, business.getOthers());
            res = insertStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res != 1) {
            logger.warn("No entrie inserted for business " + business.getHostid() + ".");
            return;
        } 
        logger.info("Successfully inserted business entry " + business.getHostid() + ".");
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
    public void update(Business business) throws DAOException {
        int res = 0;
        try {
            updateStatement.setNString(0, business.getHostid());
            updateStatement.setNString(1, business.getBusiness());
            updateStatement.setNString(2, business.getAliases());
            updateStatement.setNString(3, business.getRoles());
            updateStatement.setNString(4, business.getOthers());
            updateStatement.setNString(5, business.getHostid());
            updateStatement.setNString(6, business.getHostid());
            res = updateStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res != 1) {
            logger.warn("Unable to update business " + business.getHostid()
                    + ", entry not found.");
            return;
        } 
        logger.info("Successfully updated business entry " + business.getHostid() + ".");
    }

    private Business getFromResultSet(ResultSet set) throws SQLException {
        return new Business(
                set.getNString(HOSTID_FIELD),
                set.getNString(BUSINESS_FIELD),
                set.getNString(ROLES_FIELD),
                set.getNString(ALIASES_FIELD),
                set.getNString(OTHERS_FIELD));
    }
}

