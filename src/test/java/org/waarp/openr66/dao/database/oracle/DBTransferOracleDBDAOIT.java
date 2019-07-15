package org.waarp.openr66.dao.database.oracle;

import org.junit.ClassRule;
import org.testcontainers.containers.OracleContainer;
import org.waarp.openr66.dao.database.DBTransferDAO;
import org.waarp.openr66.dao.database.DBTransferDAOIT;
import org.waarp.openr66.dao.database.OracleTransferDAO;
import org.waarp.openr66.dao.exception.DAOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBTransferOracleDBDAOIT extends DBTransferDAOIT {

    private String createScript = "oracle/create.sql";
    private String populateScript = "oracle/populate.sql";
    private String cleanScript = "oracle/clean.sql";

    @ClassRule
    public static OracleContainer db = new OracleContainer("epiclabs/docker-oracle-xe-11g");

    @Override
    public DBTransferDAO getDAO(Connection con) throws DAOException {
        return new OracleTransferDAO(con);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                db.getJdbcUrl(),
                db.getUsername(),
                db.getPassword());
    }

    @Override
    public void initDB() {
        runScript(createScript);
        runScript(populateScript);
    }

    @Override
    public void cleanDB() {
        runScript(cleanScript);
    }
}

