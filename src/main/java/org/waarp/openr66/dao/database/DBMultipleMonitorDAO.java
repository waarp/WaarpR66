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
import org.waarp.openr66.dao.MultipleMonitorDAO;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.MultipleMonitor;

/**
 * Implementation of MultipleMonitorDAO for standard SQL databases
 */
public class DBMultipleMonitorDAO extends StatementExecutor
    implements MultipleMonitorDAO {

    private static final WaarpLogger logger = WaarpLoggerFactory.getLogger(DBMultipleMonitorDAO.class);

    protected static final String TABLE = "MULTIPLEMONITOR";

    public static final String HOSTID_FIELD = "hostid";
    public static final String COUNT_CONFIG_FIELD = "countconfig";
    public static final String COUNT_HOST_FIELD = "counthost";
    public static final String COUNT_RULE_FIELD = "countrule";

    protected static final String SQL_DELETE_ALL = "DELETE FROM " + TABLE;
    protected static final String SQL_DELETE = "DELETE FROM " + TABLE
        + " WHERE " + HOSTID_FIELD + " = ?";
    protected static final String SQL_GET_ALL = "SELECT * FROM " + TABLE;
    protected static final String SQL_EXIST = "SELECT 1 FROM " + TABLE
        + " WHERE " + HOSTID_FIELD + " = ?";
    protected static final String SQL_SELECT = "SELECT * FROM " + TABLE
        + " WHERE " + HOSTID_FIELD + " = ?";
    protected static final String SQL_INSERT = "INSERT INTO " + TABLE
        + " (" + HOSTID_FIELD + ", "
        + COUNT_CONFIG_FIELD + ", "
        + COUNT_HOST_FIELD + ", "
        + COUNT_RULE_FIELD + ") VALUES (?,?,?,?)";

    protected static final String SQL_UPDATE = "UPDATE " + TABLE
        + " SET " + HOSTID_FIELD + " = ?, "
        + COUNT_CONFIG_FIELD + " = ?, "
        + COUNT_HOST_FIELD + " = ?, "
        + COUNT_RULE_FIELD + " = ? WHERE " + HOSTID_FIELD + " = ?";

    protected Connection connection;

    public DBMultipleMonitorDAO(Connection con) {
        this.connection = con;
    }

    @Override
    public void close() {
	try {
            this.connection.close();
	} catch (SQLException e) {
            logger.warn("Cannot properly close the database connection", e);
	}
    }

    @Override
    public void delete(MultipleMonitor multipleMonitor) throws DAOException {
        PreparedStatement stm = null;
        try {
            stm = connection.prepareStatement(SQL_DELETE);
            setParameters(stm, multipleMonitor.getHostid());
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
    public List<MultipleMonitor> getAll() throws DAOException {
        ArrayList<MultipleMonitor> monitors = new ArrayList<MultipleMonitor>();
        PreparedStatement stm = null;
        ResultSet res = null;
        try {
            stm = connection.prepareStatement(SQL_GET_ALL);
            res = executeQuery(stm);
            while (res.next()) {
                monitors.add(getFromResultSet(res));
            }
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeResultSet(res);
            closeStatement(stm);
        }
        return monitors;
    }

    @Override
    public List<MultipleMonitor> find(List<Filter> filters)
            throws DAOException {
        ArrayList<MultipleMonitor> monitors = new ArrayList<MultipleMonitor>();
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
                monitors.add(getFromResultSet(res));
            }
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeResultSet(res);
            closeStatement(stm);
        }
        return monitors;
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
    public MultipleMonitor select(String hostid) throws DAOException {
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
    public void insert(MultipleMonitor multipleMonitor) throws DAOException {
        Object[] params = {
            multipleMonitor.getHostid(),
            multipleMonitor.getCountConfig(),
            multipleMonitor.getCountHost(),
            multipleMonitor.getCountRule()
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
    public void update(MultipleMonitor multipleMonitor) throws DAOException {
        Object[] params = {
            multipleMonitor.getHostid(),
            multipleMonitor.getCountConfig(),
            multipleMonitor.getCountHost(),
            multipleMonitor.getCountRule(),
            multipleMonitor.getHostid()
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

    private MultipleMonitor getFromResultSet(ResultSet set) throws SQLException {
        return new MultipleMonitor(
                set.getString(HOSTID_FIELD),
                set.getInt(COUNT_CONFIG_FIELD),
                set.getInt(COUNT_HOST_FIELD),
                set.getInt(COUNT_RULE_FIELD));
    }
}

