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
import org.waarp.openr66.dao.TransferDAO;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Transfer;

/**
 * Implementation of TransferDAO for a standard SQL database
 */
public class DBTransferDAO extends StatementExecutor implements TransferDAO {

    private static final WaarpLogger logger = WaarpLoggerFactory.getLogger(DBTransferDAO.class);

    protected static final String TABLE = "RUNNER";

    public static final String ID_FIELD = "specialid";
    public static final String GLOBAL_STEP_FIELD = "globalstep";
    public static final String GLOBAL_LAST_STEP_FIELD = "globallaststep";
    public static final String STEP_FIELD = "step";
    public static final String RANK_FIELD = "rank";
    public static final String STEP_STATUS_FIELD = "stepstatus";
    public static final String RETRIEVE_MODE_FIELD = "retrievemode";
    public static final String FILENAME_FIELD = "filename";
    public static final String IS_MOVED_FIELD = "ismoved";
    public static final String ID_RULE_FIELD = "idrule";
    public static final String BLOCK_SIZE_FIELD = "blocksz";
    public static final String ORIGINAL_NAME_FIELD = "originalname";
    public static final String FILE_INFO_FIELD = "fileinfo";
    public static final String TRANSFER_INFO_FIELD = "transferinfo";
    public static final String TRANSFER_MODE_FIELD = "modetrans";
    public static final String TRANSFER_START_FIELD = "starttrans";
    public static final String TRANSFER_STOP_FIELD = "stoptrans";
    public static final String INFO_STATUS_FIELD = "infostatus";
    public static final String OWNER_REQUEST_FIELD = "ownerreq";
    public static final String REQUESTED_FIELD = "requested";
    public static final String REQUESTER_FIELD = "requester";
    public static final String UPDATED_INFO_FIELD = "updatedInfo";

    protected static final String SQL_DELETE_ALL = "DELETE FROM " + TABLE;
    protected static String SQL_DELETE = "DELETE FROM " + TABLE
        + " WHERE " + ID_FIELD + " = ?";
    protected static final String SQL_GET_ALL = "SELECT * FROM " + TABLE;
    protected static String SQL_EXIST = "SELECT 1 FROM " + TABLE
        + " WHERE " + ID_FIELD + " = ?";
    protected static final String SQL_SELECT = "SELECT * FROM " + TABLE
        + " WHERE " + ID_FIELD + " = ?";
    protected static final String SQL_INSERT = "INSERT INTO " + TABLE
        + " (" + GLOBAL_STEP_FIELD + ", "
        + GLOBAL_LAST_STEP_FIELD + ", "
        + STEP_FIELD + ", "
        + RANK_FIELD + ", "
        + STEP_STATUS_FIELD + ", "
        + RETRIEVE_MODE_FIELD + ", "
        + FILENAME_FIELD + ", "
        + IS_MOVED_FIELD + ", "
        + ID_RULE_FIELD + ", "
        + BLOCK_SIZE_FIELD + ", "
        + ORIGINAL_NAME_FIELD + ", "
        + FILE_INFO_FIELD + ", "
        + TRANSFER_INFO_FIELD + ", "
        + TRANSFER_MODE_FIELD + ", "
        + TRANSFER_START_FIELD + ", "
        + TRANSFER_STOP_FIELD + ", "
        + INFO_STATUS_FIELD + ", "
        + OWNER_REQUEST_FIELD + ", "
        + REQUESTED_FIELD + ", "
        + REQUESTER_FIELD + ", "
        + UPDATED_INFO_FIELD + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    protected static final String SQL_UPDATE = "UPDATE " + TABLE
        + " SET " + ID_FIELD + " = ?, "
        + GLOBAL_STEP_FIELD + " = ?, "
        + GLOBAL_LAST_STEP_FIELD + " = ?, "
        + STEP_FIELD + " = ?, "
        + RANK_FIELD + " = ?, "
        + STEP_STATUS_FIELD + " = ?, "
        + RETRIEVE_MODE_FIELD + " = ?, "
        + FILENAME_FIELD + " = ?, "
        + IS_MOVED_FIELD + " = ?, "
        + ID_RULE_FIELD + " = ?, "
        + BLOCK_SIZE_FIELD + " = ?, "
        + ORIGINAL_NAME_FIELD + " = ?, "
        + FILE_INFO_FIELD + " = ?, "
        + TRANSFER_INFO_FIELD + " = ?, "
        + TRANSFER_MODE_FIELD + " = ?, "
        + TRANSFER_START_FIELD + " = ?, "
        + TRANSFER_STOP_FIELD + " = ?, "
        + INFO_STATUS_FIELD + " = ?, "
        + OWNER_REQUEST_FIELD + " = ?, "
        + REQUESTED_FIELD + " = ?, "
        + REQUESTER_FIELD + " = ?, "
        + UPDATED_INFO_FIELD + " = ?  WHERE " + ID_FIELD + " = ?";

    protected Connection connection;

    public DBTransferDAO(Connection con) {
        this.connection = con;
    }

    @Override
    public void delete(Transfer transfer) throws DAOException {
        PreparedStatement stm = null;
        try {
            stm = connection.prepareStatement(SQL_DELETE);
            setParameters(stm, transfer.getId());
            executeUpdate(stm);
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeStatement(stm);
        }
    }

    @Override
    public void deleteAll() throws DAOException{
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
    public List<Transfer> getAll() throws DAOException {
        ArrayList<Transfer> transfers = new ArrayList<Transfer>();
        PreparedStatement stm = null;
        ResultSet res = null;
        try {
            stm = connection.prepareStatement(SQL_GET_ALL);
            res = executeQuery(stm);
            while (res.next()) {
                transfers.add(getFromResultSet(res));
            }
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeResultSet(res);
            closeStatement(stm);
        }
        return transfers;
    }

    @Override
    public List<Transfer> find(List<Filter> filters) throws DAOException {
        ArrayList<Transfer> transfers = new ArrayList<Transfer>();
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
                transfers.add(getFromResultSet(res));
            }
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeResultSet(res);
            closeStatement(stm);
        }
        return transfers;
    }

    @Override
    public boolean exist(long id) throws DAOException {
        PreparedStatement stm = null;
        ResultSet res = null;
        try {
            stm = connection.prepareStatement(SQL_EXIST);
            setParameters(stm, id);
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
    public Transfer select(long id) throws DAOException {
        PreparedStatement stm = null;
        ResultSet res = null;
        try {
            stm = connection.prepareStatement(SQL_SELECT);
            setParameters(stm, id);
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
    public void insert(Transfer transfer) throws DAOException {
        Object[] params = {
            transfer.getGlobalStep().ordinal(),
            transfer.getLastGlobalStep().ordinal(),
            transfer.getStep(),
            transfer.getRank(),
            transfer.getStepStatus(),
            transfer.getRetrieveMode(),
            transfer.getFilename(),
            transfer.getIsMoved(),
            transfer.getRule(),
            transfer.getBlockSize(),
            transfer.getOriginalName(),
            transfer.getFileInfo(),
            transfer.getTransferInfo(),
            transfer.getTransferMode(),
            transfer.getStart(),
            transfer.getStop(),
            transfer.getInfoStatus(),
            transfer.getOwnerRequest(),
            transfer.getRequested(),
            transfer.getRequester(),
            transfer.getUpdatedInfo()
        };

        PreparedStatement stm = null;
        try {
            stm = connection.prepareStatement(SQL_INSERT,
                    Statement.RETURN_GENERATED_KEYS);
            setParameters(stm, params);
            executeUpdate(stm);
            transfer.setId(stm.getGeneratedKeys().getLong(ID_FIELD));
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeStatement(stm);
        }
    }

    @Override
    public void update(Transfer transfer) throws DAOException {
        Object[] params = {
            transfer.getId(),
            transfer.getGlobalStep().ordinal(),
            transfer.getLastGlobalStep().ordinal(),
            transfer.getStep(),
            transfer.getRank(),
            transfer.getStepStatus(),
            transfer.getRetrieveMode(),
            transfer.getFilename(),
            transfer.getIsMoved(),
            transfer.getRule(),
            transfer.getBlockSize(),
            transfer.getOriginalName(),
            transfer.getFileInfo(),
            transfer.getTransferInfo(),
            transfer.getTransferMode(),
            transfer.getStart(),
            transfer.getStop(),
            transfer.getInfoStatus(),
            transfer.getOwnerRequest(),
            transfer.getRequested(),
            transfer.getRequester(),
            transfer.getUpdatedInfo(),
            transfer.getId()
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

    private Transfer getFromResultSet(ResultSet set) throws SQLException {
        return new Transfer(
                set.getLong(ID_FIELD),
                set.getString(ID_RULE_FIELD),
                set.getInt(TRANSFER_MODE_FIELD),
                set.getString(FILENAME_FIELD),
                set.getString(ORIGINAL_NAME_FIELD),
                set.getString(FILE_INFO_FIELD),
                set.getBoolean(IS_MOVED_FIELD),
                set.getInt(BLOCK_SIZE_FIELD),
                set.getBoolean(RETRIEVE_MODE_FIELD),
                set.getString(OWNER_REQUEST_FIELD),
                set.getString(REQUESTER_FIELD),
                set.getString(REQUESTED_FIELD),
                set.getString(TRANSFER_INFO_FIELD),
                Transfer.TASKSTEP.valueOf(set.getInt(GLOBAL_STEP_FIELD)),
                Transfer.TASKSTEP.valueOf(set.getInt(GLOBAL_LAST_STEP_FIELD)),
                set.getInt(STEP_FIELD),
                set.getString(STEP_STATUS_FIELD),
                set.getString(INFO_STATUS_FIELD),
                set.getInt(RANK_FIELD),
                set.getTimestamp(TRANSFER_START_FIELD),
                set.getTimestamp(TRANSFER_STOP_FIELD),
                set.getInt(UPDATED_INFO_FIELD));
    }
}

