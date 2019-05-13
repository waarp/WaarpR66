package org.waarp.openr66.dao.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.waarp.openr66.dao.exception.DAOException;

public class H2TransferDAO extends DBTransferDAO {

    protected static String SQL_GET_ID = "SELECT NEXTVAL(runseq)";

    public H2TransferDAO(Connection con) throws DAOException {
        super(con);
    }

    @Override
    protected long getNextId() throws DAOException {
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(SQL_GET_ID);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            } else {
                throw new DAOException(
                        "Error no id available, you should purge the database.");
            }
        } catch (SQLException e) {
            throw new DAOException(e);
        } finally {
            closeStatement(ps);
        }
    }
}
