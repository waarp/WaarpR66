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
import org.waarp.openr66.dao.HostDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Host;

/**
 * Implementation of BusinessDAO for a standard SQL database
 */
public class DBHostDAO implements HostDAO {

    private static final WaarpLogger logger = WaarpLoggerFactory.getLogger(DBHostDAO.class);

    protected static String TABLE = "HOSTS";

    protected static String HOSTID_FIELD = "hostid";
    protected static String ADDRESS_FIELD = "address";
    protected static String PORT_FIELD = "port";
    protected static String IS_SSL_FIELD = "is_ssl";
    protected static String IS_CLIENT_FIELD = "is_client";
    protected static String IS_ACTIVE_FIELD = "is_active";
    protected static String IS_PROXIFIED_FIELD = "is_proxified";
    protected static String HOSTKEY_FIELD = "hostkey";
    protected static String ADMINROLE_FIELD = "admin_role";

    protected static String deleteQuery = "DELETE FROM " + TABLE + 
        " WHERE " + HOSTID_FIELD + " = ?";
    protected static String insertQuery = "INSERT INTO " + TABLE + 
        " (" +  ALL_FIELD() + ") VALUES (?,?,?,?,?,?,?,?,?)";
    protected static String existQuery = "SELECT 1 FROM " + TABLE + 
        " WHERE " + HOSTID_FIELD + " = ?";
    protected static String selectQuery = "SELECT * FROM " + TABLE + 
        " WHERE " + HOSTID_FIELD + " = ?";
    protected static String updateQuery = "UPDATE " + TABLE + 
        " SET " + SET_ALL_FIELD() + " WHERE " + HOSTID_FIELD + " = ?";

    protected static String ALL_FIELD() {
        return HOSTID_FIELD + ", " +
            ADDRESS_FIELD + ", " + 
            PORT_FIELD + ", " + 
            IS_SSL_FIELD + ", " + 
            IS_CLIENT_FIELD + ", " + 
            IS_ACTIVE_FIELD + ", " + 
            IS_PROXIFIED_FIELD + ", " + 
            HOSTKEY_FIELD + ", " + 
            ADMINROLE_FIELD; 
    };

    protected static String SET_ALL_FIELD() {
        return HOSTID_FIELD + " = ?, " +
            ADDRESS_FIELD + " = ?, " +
            PORT_FIELD + " = ?, " + 
            IS_SSL_FIELD + " = ?, " + 
            IS_CLIENT_FIELD + " = ?, " + 
            IS_ACTIVE_FIELD + " = ?, " + 
            IS_PROXIFIED_FIELD + " = ?, " + 
            HOSTKEY_FIELD + " = ?, " + 
            ADMINROLE_FIELD + " = ?"; 
    };

    protected Connection connection;    
    protected PreparedStatement deleteStatement;
    protected PreparedStatement insertStatement;
    protected PreparedStatement existStatement;
    protected PreparedStatement selectStatement;
    protected PreparedStatement updateStatement;

    public DBHostDAO(Connection con) throws DAOException {
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
    public void delete(Host host) throws DAOException {
        int res = 0;
        try {
            deleteStatement.setNString(0, host.getHostid());
            res = deleteStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res == 0) {
            logger.warn("Unable to delete host " + host.getHostid() 
                    + ", entry not found.");
            return;
        }
        logger.info("Successfully deleted host entry " + host.getHostid() + ".");
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
        logger.info("Successfully deleted " + res +  " host entries.");
    }

    @Override
    public List<Host> getAll() throws DAOException {
        Statement stm = null;
        ResultSet res = null;
        ArrayList<Host> hosts = new ArrayList<Host>();
        try {
            stm = connection.createStatement();
            res = stm.executeQuery("SELECT * FROM " + TABLE);
            while (res.next()) {
                hosts.add(getFromResultSet(res));
            }
            return hosts;
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
    public Host get(String hostid) throws DAOException {
        ResultSet res = null;
        try {
            selectStatement.setNString(0, hostid);
            res = selectStatement.executeQuery();
            if (!res.next()) {
                logger.warn("Unable to retrieve host " + hostid 
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
    public void insert(Host host) throws DAOException {
        int res = 0;
        try {
            insertStatement.setNString(0, host.getHostid());
            insertStatement.setNString(1, host.getAddress());
            insertStatement.setInt(2, host.getPort());
            insertStatement.setBoolean(3, host.isSSL());
            insertStatement.setBoolean(4, host.isClient());
            insertStatement.setBoolean(5, host.isActive());
            insertStatement.setBoolean(6, host.isProxified());
            insertStatement.setBytes(7, host.getHostkey());
            insertStatement.setBoolean(8, host.isAdmin());
            res = insertStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res != 1) {
            logger.warn("No entrie inserted for host " + host.getHostid() + ".");
            return;
        } 
        logger.info("Successfully inserted host entry " + host.getHostid() + ".");
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
    public void update(Host host) throws DAOException {
        int res = 0;
        try {
            updateStatement.setNString(0, host.getHostid());
            updateStatement.setNString(1, host.getAddress());
            updateStatement.setInt(2, host.getPort());
            updateStatement.setBoolean(3, host.isSSL());
            updateStatement.setBoolean(4, host.isClient());
            updateStatement.setBoolean(5, host.isActive());
            updateStatement.setBoolean(6, host.isProxified());
            updateStatement.setBytes(7, host.getHostkey());
            updateStatement.setBoolean(8, host.isAdmin());
            updateStatement.setNString(9, host.getHostid());
            res = updateStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res != 1) {
            logger.warn("Unable to update host " + host.getHostid()
                    + ", entry not found.");
            return;
        } 
        logger.info("Successfully updated host entry " + host.getHostid() + ".");
    }

    private Host getFromResultSet(ResultSet set) throws SQLException {
        return new Host(
                set.getNString(HOSTID_FIELD),
                set.getNString(ADDRESS_FIELD),
                set.getInt(PORT_FIELD),
                set.getBytes(HOSTKEY_FIELD),
                set.getBoolean(IS_SSL_FIELD),
                set.getBoolean(IS_CLIENT_FIELD),
                set.getBoolean(IS_PROXIFIED_FIELD),
                set.getBoolean(ADMINROLE_FIELD),
                set.getBoolean(IS_ACTIVE_FIELD));
    }
}

