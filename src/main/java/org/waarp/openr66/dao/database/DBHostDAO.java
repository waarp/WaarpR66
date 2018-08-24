package org.waarp.openr66.dao.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.openr66.dao.HostDAO;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.dao.exception.DataException;
import org.waarp.openr66.pojo.Host;
import org.waarp.openr66.pojo.DataError;

/**
 * Implementation of HostDAO for standard SQL databases
 */
public class DBHostDAO extends StatementExecutor implements HostDAO {

    private static final WaarpLogger logger = WaarpLoggerFactory.getLogger(DBHostDAO.class);

    protected static final String TABLE = "HOSTS";

    public static final String HOSTID_FIELD = "hostid";
    public static final String ADDRESS_FIELD = "address";
    public static final String PORT_FIELD = "port";
    public static final String IS_SSL_FIELD = "isssl";
    public static final String IS_CLIENT_FIELD = "isclient";
    public static final String IS_ACTIVE_FIELD = "isactive";
    public static final String IS_PROXIFIED_FIELD = "isproxified";
    public static final String HOSTKEY_FIELD = "hostkey";
    public static final String ADMINROLE_FIELD = "adminrole";
    public static final String UPDATED_INFO_FIELD = "updatedinfo";

    protected static final String SQL_DELETE_ALL = "DELETE FROM " + TABLE;
    protected static String SQL_DELETE = "DELETE FROM " + TABLE 
        + " WHERE " + HOSTID_FIELD + " = ?";
    protected static final String SQL_GET_ALL = "SELECT * FROM " + TABLE;
    protected static String SQL_EXIST = "SELECT 1 FROM " + TABLE 
        + " WHERE " + HOSTID_FIELD + " = ?";
    protected static String SQL_SELECT = "SELECT * FROM " + TABLE 
        + " WHERE " + HOSTID_FIELD + " = ?";
    protected static final String SQL_INSERT = "INSERT INTO " + TABLE + 
        " (" + HOSTID_FIELD + ", "
        + ADDRESS_FIELD + ", "
        + PORT_FIELD + ", "
        + IS_SSL_FIELD + ", "
        + IS_CLIENT_FIELD + ", "
        + IS_ACTIVE_FIELD + ", "
        + IS_PROXIFIED_FIELD + ", "
        + HOSTKEY_FIELD + ", "
        + ADMINROLE_FIELD + ", " 
        + UPDATED_INFO_FIELD + ") VALUES (?,?,?,?,?,?,?,?,?,?)";
    protected static String SQL_UPDATE = "UPDATE " + TABLE + 
        " SET " + HOSTID_FIELD + " = ?, "
        + ADDRESS_FIELD + " = ?, "
        + PORT_FIELD + " = ?, "
        + IS_SSL_FIELD + " = ?, "
        + IS_CLIENT_FIELD + " = ?, "
        + IS_ACTIVE_FIELD + " = ?, "
        + IS_PROXIFIED_FIELD + " = ?, "
        + HOSTKEY_FIELD + " = ?, "
        + ADMINROLE_FIELD + " = ?, "
        + UPDATED_INFO_FIELD + " = ? WHERE " + HOSTID_FIELD + " = ?";

    protected Connection connection;    

    public DBHostDAO(Connection con) throws DAOException {
        this.connection = con;
    }

    @Override
    public void delete(Host host) throws DAOException {
        PreparedStatement stm = null;
        try {
            stm = connection.prepareStatement(SQL_DELETE);
            setParameters(stm, host.getHostid());
            executeUpdate(stm);
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeStatement(stm);
        }
    }

    @Override
    public void deleteAll() throws DAOException {
        PreparedStatement stm = null;
        try {
            stm = connection.prepareStatement(SQL_DELETE_ALL);
            executeUpdate(stm);
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeStatement(stm);
        }
    }

    @Override
    public List<Host> getAll() throws DAOException {
        ArrayList<Host> hosts = new ArrayList<Host>();
        PreparedStatement stm = null;
        ResultSet res = null;
        try {
            stm = connection.prepareStatement(SQL_GET_ALL);
            res = executeQuery(stm);
            while (res.next()) {
                hosts.add(getFromResultSet(res));
            }
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeResultSet(res);
            closeStatement(stm);
        }
        return hosts;
    }

    @Override
    public List<Host> find(List<Filter> filters) throws DAOException {
        ArrayList<Host> hosts = new ArrayList<Host>();
        // Create the SQL query
        StringBuilder query = new StringBuilder(SQL_GET_ALL);
        Object[] params = new Object[filters.size()];
        Iterator<Filter> it = filters.listIterator();
        if (it.hasNext()) {
            query.append(" WHERE ");
        }
        String prefix = "";
        int i = 0;
        while (it.hasNext()) {
            query.append(prefix); 
            Filter filter = it.next();
            query.append(filter.key + " " + filter.operand + " ?"); 
            params[i] = filter.value;
            i++;
            prefix = " AND ";
        }
        // Execute query
        PreparedStatement stm = null;
        ResultSet res = null;
        try {
            stm = connection.prepareStatement(query.toString());
            setParameters(stm, params);
            res = executeQuery(stm);
            while (res.next()) {
                hosts.add(getFromResultSet(res));
            }
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeResultSet(res);
            closeStatement(stm);
        }
        return hosts;
    }

    @Override
    public boolean exist(String hostid) throws DAOException {
        PreparedStatement stm = null;
        ResultSet res = null;
        try {
            stm = connection.prepareStatement(SQL_EXIST);
            setParameters(stm, hostid);
            res = executeQuery(stm);
            return res.next();
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeResultSet(res);
            closeStatement(stm);
        }
    }

    @Override
    public Host select(String hostid) throws DAOException {
        PreparedStatement stm = null;
        ResultSet res = null;
        try {
            stm = connection.prepareStatement(SQL_SELECT);
            setParameters(stm, hostid);
            res = executeQuery(stm);
            if (res.next()) {
                return getFromResultSet(res);
            }
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeResultSet(res);
            closeStatement(stm);
        }
        return null;
    }

    @Override
    public void insert(Host host) throws DAOException, DataException {
        DataError err = host.validate();
        if (err.isError()) {
            throw new DataException("Invalid data", err);       
        }

        Object[] params = {
            host.getHostid(),
            host.getAddress(),
            host.getPort(),
            host.isSSL(),
            host.isClient(),
            host.isActive(),
            host.isProxified(),
            host.getHostkey(),
            host.isAdmin(),
            host.getUpdatedInfo()
        };

        PreparedStatement stm = null;
        try {
            stm = connection.prepareStatement(SQL_INSERT);
            setParameters(stm, params);
            executeUpdate(stm);
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeStatement(stm);
        }
    }

    @Override
    public void update(Host host) throws DAOException, DataException {
        DataError err = host.validate();
        if (err.isError()) {
            throw new DataException("Invalid data", err);       
        }

        Object[] params = {
            host.getHostid(),
            host.getAddress(),
            host.getPort(),
            host.isSSL(),
            host.isClient(),
            host.isActive(),
            host.isProxified(),
            host.getHostkey(),
            host.isAdmin(),
            host.getUpdatedInfo(),
            host.getHostid()
        };

        PreparedStatement stm = null;
        try {
            stm = connection.prepareStatement(SQL_UPDATE);
            setParameters(stm, params);
            executeUpdate(stm);
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeStatement(stm);
        }
    }

    private Host getFromResultSet(ResultSet set) throws SQLException {
        return new Host(
                set.getString(HOSTID_FIELD),
                set.getString(ADDRESS_FIELD),
                set.getInt(PORT_FIELD),
                set.getBytes(HOSTKEY_FIELD),
                set.getBoolean(IS_SSL_FIELD),
                set.getBoolean(IS_CLIENT_FIELD),
                set.getBoolean(IS_PROXIFIED_FIELD),
                set.getBoolean(ADMINROLE_FIELD),
                set.getBoolean(IS_ACTIVE_FIELD),
                set.getInt(UPDATED_INFO_FIELD));
    }
}

