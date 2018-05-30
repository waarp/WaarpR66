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
import org.waarp.openr66.dao.LimitDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Limit;

public class DBLimitDAO implements LimitDAO {

    private static final WaarpLogger logger = WaarpLoggerFactory.getLogger(LimitDAO.class);

    protected static String TABLE = "CONFIGURATION";

    protected static String HOSTID_FIELD = "hostid";
    protected static String READ_GLOBAL_LIMIT_FIELD = "readgloballimit";
    protected static String WRITE_GLOBAL_LIMIT_FIELD = "writegloballimit";
    protected static String READ_SESSION_LIMIT_FIELD = "readsessionlimit";
    protected static String WRITE_SESSION_LIMIT_FIELD = "writesessionlimit";
    protected static String DELAY_LIMIT_FIELD = "delaylimit";

    protected static String deleteQuery = "DELETE FROM " + TABLE + 
        " WHERE " + HOSTID_FIELD + " = ?";
    protected static String insertQuery = "INSERT INTO " + TABLE + 
        " (" +  ALL_FIELD() + ") VALUES (?,?,?,?,?,?)";
    protected static String existQuery = "SELECT 1 FROM " + TABLE + 
        " WHERE " + HOSTID_FIELD + " = ?";
    protected static String selectQuery = "SELECT * FROM " + TABLE + 
        " WHERE " + HOSTID_FIELD + " = ?";
    protected static String updateQuery = "UPDATE " + TABLE + 
        " SET " + SET_ALL_FIELD() + " WHERE " + HOSTID_FIELD + " = ?";

    protected static String ALL_FIELD() {
        return HOSTID_FIELD + ", " +
            READ_GLOBAL_LIMIT_FIELD + ", " + 
            WRITE_GLOBAL_LIMIT_FIELD + ", " + 
            READ_SESSION_LIMIT_FIELD + ", " + 
            WRITE_SESSION_LIMIT_FIELD + ", " + 
            DELAY_LIMIT_FIELD; 
    }

    protected static String SET_ALL_FIELD() {
        return HOSTID_FIELD + " = ?, " +
            READ_GLOBAL_LIMIT_FIELD + " = ?, " + 
            WRITE_GLOBAL_LIMIT_FIELD + " = ?, " + 
            READ_SESSION_LIMIT_FIELD + " = ?, " + 
            WRITE_SESSION_LIMIT_FIELD + " = ?, " + 
            DELAY_LIMIT_FIELD + " = ?"; 
    }

    protected Connection connection;
    protected PreparedStatement deleteStatement;
    protected PreparedStatement insertStatement;
    protected PreparedStatement existStatement;
    protected PreparedStatement selectStatement;
    protected PreparedStatement updateStatement;

    public DBLimitDAO(Connection con) throws DAOException {
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
    public void delete(Limit limit) throws DAOException {
        int res = 0;
        try {
            deleteStatement.setNString(0, limit.getHostid());
            res = deleteStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res == 0) {
            logger.warn("Unable to delete limit " + limit.getHostid() 
                    + ", entry not found.");
            return;
        }
        logger.info("Successfully deleted limit entry " + limit.getHostid() + ".");
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
        logger.info("Successfully deleted " + res +  " limit entries.");
    }

    @Override
    public List<Limit> getAll() throws DAOException {
        Statement stm = null;
        ResultSet res = null;
        ArrayList<Limit> limits = new ArrayList<Limit>();
        try {
            stm = connection.createStatement();
            res = stm.executeQuery("SELECT * FROM " + TABLE);
            while (res.next()) {
                limits.add(getFromResultSet(res));
            }
            return limits;
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
    public Limit get(String hostid) throws DAOException {
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
    public void insert(Limit limit) throws DAOException {
        int res = 0;
        try {
            insertStatement.setNString(0, limit.getHostid());
            insertStatement.setLong(1, limit.getReadGlobalLimit());
            insertStatement.setLong(2, limit.getWriteGlobalLimit());
            insertStatement.setLong(3, limit.getReadSessionLimit());
            insertStatement.setLong(4, limit.getWriteSessionLimit());
            insertStatement.setLong(5, limit.getDelayLimit());
            res = insertStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res != 1) {
            logger.warn("No entrie inserted for limit " + limit.getHostid() + ".");
            return;
        } 
        logger.info("Successfully inserted limit entry " + limit.getHostid() + ".");
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
    public void update(Limit limit) throws DAOException {
        int res = 0;
        try {
            updateStatement.setNString(0, limit.getHostid());
            updateStatement.setLong(1, limit.getReadGlobalLimit());
            updateStatement.setLong(2, limit.getWriteGlobalLimit());
            updateStatement.setLong(3, limit.getReadSessionLimit());
            updateStatement.setLong(4, limit.getWriteSessionLimit());
            updateStatement.setLong(5, limit.getDelayLimit());
            res = updateStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res != 1) {
            logger.warn("Unable to update limit " + limit.getHostid()
                    + ", entry not found.");
            return;
        } 
        logger.info("Successfully updated limit entry " + limit.getHostid() + ".");
    }

    private Limit getFromResultSet(ResultSet set) throws SQLException {
        return new Limit(
                set.getNString(HOSTID_FIELD),
                set.getLong(READ_GLOBAL_LIMIT_FIELD),
                set.getLong(WRITE_GLOBAL_LIMIT_FIELD),
                set.getLong(READ_SESSION_LIMIT_FIELD),
                set.getLong(WRITE_SESSION_LIMIT_FIELD),
                set.getLong(DELAY_LIMIT_FIELD));
    }
}
