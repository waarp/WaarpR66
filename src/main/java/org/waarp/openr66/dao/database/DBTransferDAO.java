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
import org.waarp.openr66.dao.TransferDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Transfer;

/**
 * Implementation of TransferDAO for a standard SQL database
 */
public class DBTransferDAO implements TransferDAO {

    private static final WaarpLogger logger = WaarpLoggerFactory.getLogger(DBTransferDAO.class);

    protected static String TABLE = "TASKRUNNERS";

    protected static String ID_FIELD = "specialid";
    protected static String GLOBAL_STEP_FIELD = "globalstep";
    protected static String GLOBAL_LAST_STEP_FIELD = "globallaststep";
    protected static String STEP_FIELD = "step";
    protected static String RANK_FIELD = "rank";
    protected static String STEP_STATUS_FIELD = "stepstatus";
    protected static String RETRIEVE_MODE_FIELD = "retrievemode";
    protected static String FILENAME_FIELD = "filename";
    protected static String IS_MOVED_FIELD = "ismoved";
    protected static String ID_RULE_FIELD = "idrule";
    protected static String BLOCK_SIZE_FIELD = "blocks";
    protected static String ORIGINAL_NAME_FIELD = "originalname";
    protected static String FILE_INFO_FIELD = "fileinfo";
    protected static String TRANSFER_INFO_FIELD = "transferinfo";
    protected static String TRANSFER_MODE_FIELD = "modetrans";
    protected static String TRANSFER_START_FIELD = "transferstart";
    protected static String TRANSFER_STOP_FIELD = "transferstop";
    protected static String INFO_STATUS_FIELD = "infostatus";
    protected static String OWNER_REQUEST_FIELD = "ownerreq";
    protected static String REQUESTED_FIELD = "requested";
    protected static String REQUESTER_FIELD = "requester";

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
            GLOBAL_STEP_FIELD + ", " + 
            GLOBAL_LAST_STEP_FIELD + ", " + 
            STEP_FIELD + ", " + 
            RANK_FIELD + ", " + 
            STEP_STATUS_FIELD + ", " + 
            RETRIEVE_MODE_FIELD + ", " + 
            FILENAME_FIELD + ", "+
            IS_MOVED_FIELD + ", "+
            ID_RULE_FIELD + ", "+
            BLOCK_SIZE_FIELD + ", "+
            ORIGINAL_NAME_FIELD + ", "+ 
            FILE_INFO_FIELD + ", "+ 
            TRANSFER_INFO_FIELD + ", "+ 
            TRANSFER_MODE_FIELD + ", "+
            TRANSFER_START_FIELD + ", "+
            TRANSFER_STOP_FIELD + ", "+
            INFO_STATUS_FIELD + ", "+
            OWNER_REQUEST_FIELD + ", "+
            REQUESTED_FIELD + ", "+
            REQUESTER_FIELD;
    }

    protected static String SET_ALL_FIELD() {
        return ID_FIELD + " = ?," +
            GLOBAL_STEP_FIELD + " = ?," + 
            GLOBAL_LAST_STEP_FIELD + " = ?," + 
            STEP_FIELD + " = ?," + 
            RANK_FIELD + " = ?," + 
            STEP_STATUS_FIELD + " = ?," + 
            RETRIEVE_MODE_FIELD + " = ?," + 
            FILENAME_FIELD + " = ?,"+
            IS_MOVED_FIELD + " = ?,"+
            ID_RULE_FIELD + " = ?,"+
            BLOCK_SIZE_FIELD + " = ?,"+
            ORIGINAL_NAME_FIELD + " = ?,"+ 
            FILE_INFO_FIELD + " = ?,"+ 
            TRANSFER_INFO_FIELD + " = ?,"+ 
            TRANSFER_MODE_FIELD + " = ?,"+
            TRANSFER_START_FIELD + " = ?,"+
            TRANSFER_STOP_FIELD + " = ?,"+
            INFO_STATUS_FIELD + " = ?,"+
            OWNER_REQUEST_FIELD + " = ?,"+
            REQUESTED_FIELD + " = ?,"+
            REQUESTER_FIELD + " = ?";
    }

    protected Connection connection;    
    protected PreparedStatement deleteStatement;
    protected PreparedStatement insertStatement;
    protected PreparedStatement existStatement;
    protected PreparedStatement selectStatement;
    protected PreparedStatement updateStatement;

    public DBTransferDAO(Connection con) throws DAOException {
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
    public void delete(Transfer transfer) throws DAOException {
        int res = 0;
        try {
            deleteStatement.setLong(0, transfer.getId());
            res = deleteStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res == 0) {
            logger.warn("Unable to delete transfer " + transfer.getId() 
                    + ", entry not found.");
            return;
        }
        logger.info("Successfully deleted transfer entry " + transfer.getId() + ".");
    }

    @Override
    public void deleteAll() throws DAOException{
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
        logger.info("Successfully deleted " + res +  " transfer entries.");
    }

    @Override
    public List<Transfer> getAll() throws DAOException {
        Statement stm = null;
        ResultSet res = null;
        ArrayList<Transfer> transfers = new ArrayList<Transfer>();
        try {
            stm = connection.createStatement();
            res = stm.executeQuery("SELECT * FROM " + TABLE);
            while (res.next()) {
                transfers.add(getFromResultSet(res));
            }
            return transfers;
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
    public Transfer get(long id) throws DAOException {
        ResultSet res = null;
        try {
            selectStatement.setLong(0, id);
            res = selectStatement.executeQuery();
            if (!res.next()) {
                logger.warn("Unable to retrieve transfer " + id 
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
    public void insert(Transfer transfer) throws DAOException {
        int res = 0;
        try {
            insertStatement.setLong(0, transfer.getId());
            insertStatement.setInt(1, transfer.getGlobalStep().ordinal());
            insertStatement.setInt(2, transfer.getLastGlobalStep().ordinal());
            insertStatement.setInt(3, transfer.getStep());
            insertStatement.setInt(4, transfer.getRank());
            insertStatement.setNString(5, transfer.getStepStatus());
            insertStatement.setBoolean(6, transfer.getRetrieveMode());
            insertStatement.setNString(7, transfer.getFilename());
            insertStatement.setBoolean(8, transfer.getIsMoved());
            insertStatement.setNString(9, transfer.getRule());
            insertStatement.setInt(10, transfer.getBlockSize());
            insertStatement.setNString(11, transfer.getOriginalName());
            insertStatement.setNString(12, transfer.getFileInfo());
            insertStatement.setNString(13, transfer.getTransferInfo());
            insertStatement.setInt(14, transfer.getTransferMode());
            insertStatement.setTimestamp(15, transfer.getStart());
            insertStatement.setTimestamp(16, transfer.getStop());
            insertStatement.setNString(17, transfer.getInfoStatus());
            insertStatement.setNString(18, transfer.getOwnerRequest());
            insertStatement.setNString(19, transfer.getRequested());
            insertStatement.setNString(20, transfer.getRequester());
            res = insertStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res != 1) {
            logger.warn("No entry inserted for transfer " + transfer.getId() + ".");
            return;
        } 
        logger.info("Successfully inserted transfer entry " + transfer.getId() + ".");
    }

    @Override
    public boolean exist(long id) throws DAOException {
        ResultSet res = null;
        try {
            existStatement.setLong(0, id);
            res = selectStatement.executeQuery();
            return res.isBeforeFirst();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        } finally {
            try {
                res.close();
            } catch (SQLException e) {
                logger.warn("An error occurs while atempting to close the ResultSet.", e);
            }
        }
    }


    @Override
    public void update(Transfer transfer) throws DAOException {
        int res = 0;
        try {
            updateStatement.setLong(0, transfer.getId());
            updateStatement.setInt(1, transfer.getGlobalStep().ordinal());
            updateStatement.setInt(2, transfer.getLastGlobalStep().ordinal());
            updateStatement.setInt(3, transfer.getStep());
            updateStatement.setInt(4, transfer.getRank());
            updateStatement.setNString(5, transfer.getStepStatus());
            updateStatement.setBoolean(6, transfer.getRetrieveMode());
            updateStatement.setNString(7, transfer.getFilename());
            updateStatement.setBoolean(8, transfer.getIsMoved());
            updateStatement.setNString(9, transfer.getRule());
            updateStatement.setInt(10, transfer.getBlockSize());
            updateStatement.setNString(11, transfer.getOriginalName());
            updateStatement.setNString(12, transfer.getFileInfo());
            updateStatement.setNString(13, transfer.getTransferInfo());
            updateStatement.setInt(14, transfer.getTransferMode());
            updateStatement.setTimestamp(15, transfer.getStart());
            updateStatement.setTimestamp(16, transfer.getStop());
            updateStatement.setNString(17, transfer.getInfoStatus());
            updateStatement.setNString(18, transfer.getOwnerRequest());
            updateStatement.setNString(19, transfer.getRequested());
            updateStatement.setNString(20, transfer.getRequester());
            res = updateStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Data access error.", e);
        }
        if (res != 1) {
            logger.warn("Unable to update transfer" + transfer.getId() 
                    + ", entry not found.");
            return;
        } 
        logger.info("Successfully updated transfer entry " + transfer.getId() + ".");
    }

    private Transfer getFromResultSet(ResultSet set) throws SQLException {
        return new Transfer(
                set.getLong(ID_FIELD),
                set.getNString(ID_RULE_FIELD), 
                set.getInt(TRANSFER_MODE_FIELD), 
                set.getNString(FILENAME_FIELD), 
                set.getNString(ORIGINAL_NAME_FIELD),  
                set.getNString(FILE_INFO_FIELD),  
                set.getBoolean(IS_MOVED_FIELD), 
                set.getInt(BLOCK_SIZE_FIELD), 
                set.getBoolean(RETRIEVE_MODE_FIELD),  
                set.getNString(OWNER_REQUEST_FIELD), 
                set.getNString(REQUESTER_FIELD),
                set.getNString(REQUESTED_FIELD), 
                set.getNString(TRANSFER_INFO_FIELD),  
                Transfer.TASKSTEP.valueOf(set.getInt(GLOBAL_STEP_FIELD)),  
                Transfer.TASKSTEP.valueOf(set.getInt(GLOBAL_LAST_STEP_FIELD)),  
                set.getInt(STEP_FIELD),  
                set.getNString(STEP_STATUS_FIELD),  
                set.getNString(INFO_STATUS_FIELD), 
                set.getInt(RANK_FIELD),  
                set.getTimestamp(TRANSFER_START_FIELD), 
                set.getTimestamp(TRANSFER_STOP_FIELD)); 
    }
}

