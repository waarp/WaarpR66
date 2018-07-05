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
import org.waarp.openr66.dao.RuleDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Rule;
import org.waarp.openr66.pojo.RuleTask;

/**
 * Implementation of RuleDAO for a standard SQL database
 */
public class DBRuleDAO implements RuleDAO {

    private static final WaarpLogger logger = WaarpLoggerFactory.getLogger(DBRuleDAO.class);

    protected static String TABLE = "RULES";

    protected static String ID_FIELD = "idrule";
    protected static String HOSTIDS_FIELD = "hostids";
    protected static String MODE_TRANS_FIELD = "modetrans";
    protected static String RECV_PATH_FIELD = "recvpath";
    protected static String SEND_PATH_FIELD = "sendpath";
    protected static String ARCHIVE_PATH_FIELD = "archivepath";
    protected static String WORK_PATH_FIELD = "workpath";
    protected static String R_PRE_TASKS_FIELD = "rpretask";
    protected static String R_POST_TASKS_FIELD = "rposttask";
    protected static String R_ERROR_TASKS_FIELD = "rerrortask";
    protected static String S_PRE_TASKS_FIELD = "sprestask";
    protected static String S_POST_TASKS_FIELD = "sposttask";
    protected static String S_ERROR_TASKS_FIELD = "serrortask";

    protected static String deleteQuery = "DELETE FROM " + TABLE + 
        " WHERE " + ID_FIELD + " = ?";
    protected static String insertQuery = "INSERT INTO " + TABLE + 
        " (" +  ALL_FIELD() + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
    protected static String existQuery = "SELECT 1 FROM " + TABLE + 
        " WHERE " + ID_FIELD + " = ?";
    protected static String selectQuery = "SELECT * FROM " + TABLE + 
        " WHERE " + ID_FIELD + " = ?";
    protected static String updateQuery = "UPDATE " + TABLE + 
        " SET " + SET_ALL_FIELD() + " WHERE " + ID_FIELD + " = ?";

    protected static String ALL_FIELD() {
        return ID_FIELD + ", " + 
            HOSTIDS_FIELD + ", " + 
            MODE_TRANS_FIELD + ", " + 
            RECV_PATH_FIELD + ", " + 
            SEND_PATH_FIELD + ", " + 
            ARCHIVE_PATH_FIELD + ", " + 
            WORK_PATH_FIELD + ", " + 
            R_PRE_TASKS_FIELD + ", " + 
            R_POST_TASKS_FIELD + ", " + 
            R_ERROR_TASKS_FIELD + ", " + 
            S_PRE_TASKS_FIELD + ", " + 
            S_POST_TASKS_FIELD; 
    }

    protected static String SET_ALL_FIELD() {
        return ID_FIELD + " = ?, " + 
            HOSTIDS_FIELD + " = ?, " + 
            MODE_TRANS_FIELD + " = ? ," + 
            RECV_PATH_FIELD + " = ?, " + 
            SEND_PATH_FIELD + " = ?, " + 
            ARCHIVE_PATH_FIELD + " = ? ," + 
            WORK_PATH_FIELD + " = ? ," + 
            R_PRE_TASKS_FIELD + " = ? ," + 
            R_POST_TASKS_FIELD + " = ? ," + 
            R_ERROR_TASKS_FIELD + " = ? ," + 
            S_PRE_TASKS_FIELD + " = ? ," + 
            S_POST_TASKS_FIELD + " = ? ," + 
            S_ERROR_TASKS_FIELD + " = ?"; 
    }

    protected Connection connection;    
    protected PreparedStatement deleteStatement;
    protected PreparedStatement insertStatement;
    protected PreparedStatement existStatement;
    protected PreparedStatement selectStatement;
    protected PreparedStatement updateStatement;

    public DBRuleDAO(Connection con) throws DAOException {
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
    public void delete(Rule rule) throws DAOException {
        int res = 0;
        try {
            deleteStatement.setNString(0, rule.getName());
            res = deleteStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res == 0) {
            logger.warn("Unable to delete rule " + rule.getName()
                    + ", entry not found.");
            return;
        }
        logger.info("Successfully deleted rule entry " + rule.getName() + ".");
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
        logger.info("Successfully deleted " + res +  " rule entries.");
    }

    @Override
    public List<Rule> getAll() throws DAOException {
        Statement stm = null;
        ResultSet res = null;
        ArrayList<Rule> rules = new ArrayList<Rule>();
        try {
            stm = connection.createStatement();
            res = stm.executeQuery("SELECT * FROM " + TABLE);
            while (res.next()) {
                rules.add(getFromResultSet(res));
            }
            return rules;
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
    public Rule get(String ruleName) throws DAOException {
        ResultSet res = null;
        try {
            selectStatement.setNString(0, ruleName);
            res = selectStatement.executeQuery();
            if (!res.next()) {
                logger.warn("Unable to retrieve rule " + ruleName
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
    public void insert(Rule rule) throws DAOException {
        int res = 0;
        try {
            insertStatement.setNString(0, rule.getName());
            insertStatement.setInt(1, rule.getMode());
            insertStatement.setNString(2, rule.getName());
            insertStatement.setNString(3, rule.getXMLHostids());
            insertStatement.setNString(4, rule.getRecvPath());
            insertStatement.setNString(5, rule.getSendPath());
            insertStatement.setNString(6, rule.getArchivePath());
            insertStatement.setNString(7, rule.getWorkPath());
            insertStatement.setNString(8, rule.getXMLRPreTasks());
            insertStatement.setNString(9, rule.getXMLRPostTasks());
            insertStatement.setNString(10, rule.getXMLRErrorTasks());
            insertStatement.setNString(11, rule.getXMLSPreTasks());
            insertStatement.setNString(12, rule.getXMLSPostTasks());
            insertStatement.setNString(13, rule.getXMLSErrorTasks());
            res = insertStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res != 1) {
            logger.warn("No entrie inserted for rule " + rule.getName() + ".");
            return;
        } 
        logger.info("Successfully inserted rule entry " + rule.getName() + ".");
    }

    @Override
    public boolean exist(String ruleName) throws DAOException {
        ResultSet res = null;
        try {
            existStatement.setNString(0, ruleName);
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
    public void update(Rule rule) throws DAOException {
        int res = 0;
        try {
            updateStatement.setNString(0, rule.getName());
            updateStatement.setInt(1, rule.getMode());
            updateStatement.setNString(2, rule.getName());
            updateStatement.setNString(3, rule.getXMLHostids());
            updateStatement.setNString(4, rule.getRecvPath());
            updateStatement.setNString(5, rule.getSendPath());
            updateStatement.setNString(6, rule.getArchivePath());
            updateStatement.setNString(7, rule.getWorkPath());
            updateStatement.setNString(8, rule.getXMLRPreTasks());
            updateStatement.setNString(9, rule.getXMLRPostTasks());
            updateStatement.setNString(10, rule.getXMLRErrorTasks());
            updateStatement.setNString(11, rule.getXMLSPreTasks());
            updateStatement.setNString(12, rule.getXMLSPostTasks());
            updateStatement.setNString(13, rule.getXMLSErrorTasks());
            updateStatement.setNString(14, rule.getName());
            res = updateStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res != 1) {
            logger.warn("Unable to update rule" + rule.getName()
                    + ", entry not found.");
            return;
        } 
        logger.info("Successfully updated rule entry " + rule.getName() + ".");
    }

    private Rule getFromResultSet(ResultSet set) throws SQLException {
        return new Rule(
                set.getNString(ID_FIELD),
                set.getInt(MODE_TRANS_FIELD),
                retrieveHostids(set.getNString(HOSTIDS_FIELD)),
                set.getNString(RECV_PATH_FIELD),
                set.getNString(SEND_PATH_FIELD),
                set.getNString(ARCHIVE_PATH_FIELD),
                set.getNString(WORK_PATH_FIELD),
                retrieveTasks(set.getNString(R_PRE_TASKS_FIELD)),
                retrieveTasks(set.getNString(R_POST_TASKS_FIELD)),
                retrieveTasks(set.getNString(R_ERROR_TASKS_FIELD)),
                retrieveTasks(set.getNString(S_PRE_TASKS_FIELD)),
                retrieveTasks(set.getNString(S_POST_TASKS_FIELD)),
                retrieveTasks(set.getNString(S_ERROR_TASKS_FIELD))
                );
    }

    private List<String> retrieveHostids(String xml) {
        //TODO implement retrieveHostids
        return new ArrayList<String>();
    }

    private List<RuleTask> retrieveTasks(String xml) {
        //TODO implement retrieveTasks
        return new ArrayList<RuleTask>();
    }
}

