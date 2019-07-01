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
import org.waarp.openr66.dao.BusinessDAO;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Business;

/**
 * Implementation of BusinessDAO for standard SQL databases
 */
public class DBBusinessDAO extends StatementExecutor implements BusinessDAO {

    private static final WaarpLogger logger = WaarpLoggerFactory.getLogger(DBBusinessDAO.class);

    protected static final String TABLE = "HOSTCONFIG";

    public static final String HOSTID_FIELD = "hostid";
    public static final String BUSINESS_FIELD = "business";
    public static final String ROLES_FIELD = "roles";
    public static final String ALIASES_FIELD = "aliases";
    public static final String OTHERS_FIELD = "others";
    public static final String UPDATED_INFO_FIELD = "updatedInfo";

    protected static final String SQL_DELETE_ALL = "DELETE FROM " + TABLE;
    protected static final String SQL_DELETE = "DELETE FROM " + TABLE
        + " WHERE " + HOSTID_FIELD + " = ?";
    protected static final  String SQL_GET_ALL = "SELECT * FROM " + TABLE;
    protected static final String SQL_EXIST = "SELECT 1 FROM " + TABLE
        + " WHERE " + HOSTID_FIELD + " = ?";
    protected static final String SQL_SELECT = "SELECT * FROM " + TABLE
        + " WHERE " + HOSTID_FIELD + " = ?";
    protected static final String SQL_INSERT = "INSERT INTO " + TABLE
        + " (" + HOSTID_FIELD + ", "
        + BUSINESS_FIELD + ", "
        + ROLES_FIELD + ", "
        + ALIASES_FIELD + ", "
        + OTHERS_FIELD + ", "
        + UPDATED_INFO_FIELD + ") VALUES (?,?,?,?,?,?)";
    protected static final String SQL_UPDATE = "UPDATE " + TABLE
        + " SET " + HOSTID_FIELD + " = ?, "
        + BUSINESS_FIELD + " = ?, "
        + ROLES_FIELD + " = ?, "
        + ALIASES_FIELD + " = ?, "
        + OTHERS_FIELD + " = ?, "
        + UPDATED_INFO_FIELD + " = ? WHERE " + HOSTID_FIELD + " = ?";

    protected Connection connection;

    public DBBusinessDAO(Connection con) {
        this.connection = con;
    }

    @Override
    public void delete(Business business) throws DAOException {
        PreparedStatement stm = null;
        try {
            stm = connection.prepareStatement(SQL_DELETE);
            setParameters(stm, business.getHostid());
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
    public List<Business> getAll() throws DAOException {
        ArrayList<Business> businesses = new ArrayList<Business>();
        PreparedStatement stm = null;
        ResultSet res = null;
        try {
            stm = connection.prepareStatement(SQL_GET_ALL);
            res = executeQuery(stm);
            while (res.next()) {
                businesses.add(getFromResultSet(res));
            }
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeResultSet(res);
            closeStatement(stm);
        }
        return businesses;
    }

    @Override
    public List<Business> find(List<Filter> filters) throws DAOException {
        ArrayList<Business> businesses = new ArrayList<Business>();
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
                businesses.add(getFromResultSet(res));
            }
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeResultSet(res);
            closeStatement(stm);
        }
        return businesses;
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
    public Business select(String hostid) throws DAOException {
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
    public void insert(Business business) throws DAOException {
        Object[] params = {
            business.getHostid(),
            business.getBusiness(),
            business.getRoles(),
            business.getAliases(),
            business.getOthers(),
            business.getUpdatedInfo()
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
    public void update(Business business) throws DAOException {
        Object[] params = {
            business.getHostid(),
            business.getBusiness(),
            business.getRoles(),
            business.getAliases(),
            business.getOthers(),
            business.getUpdatedInfo(),
            business.getHostid()
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

    private Business getFromResultSet(ResultSet set) throws SQLException {
        return new Business(
                set.getString(HOSTID_FIELD),
                set.getString(BUSINESS_FIELD),
                set.getString(ROLES_FIELD),
                set.getString(ALIASES_FIELD),
                set.getString(OTHERS_FIELD),
                set.getInt(UPDATED_INFO_FIELD));
    }
}

