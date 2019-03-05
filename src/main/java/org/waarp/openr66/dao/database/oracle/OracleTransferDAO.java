package org.waarp.openr66.dao.database;

import java.sql.Connection;

import org.waarp.openr66.dao.exception.DAOException;

public class OracleTransferDAO extends DBTransferDAO {

    protected static String SQL_GET_ID = "SELECT runseq.nextval FROM DUAL";

    public OracleTransferDAO(Connection con) throws DAOException {
        super(con);
    }

    @Override
    protected String getSequenceRequest() {
        return SQL_GET_ID;
    }
}
