package org.waarp.openr66.dao.database;

import java.sql.Connection;

import org.waarp.openr66.dao.exception.DAOException;

public class PostgreSQLTransferDAO extends DBTransferDAO {

    protected static String SQL_GET_ID = "SELECT NEXTVAL(runseq)";

    public PostgreSQLTransferDAO(Connection con) throws DAOException {
        super(con);
    }

    @Override
    protected String getSequenceRequest() {
        return SQL_GET_ID;
    }
}
