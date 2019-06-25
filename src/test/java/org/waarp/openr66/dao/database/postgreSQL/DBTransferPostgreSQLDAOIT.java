package org.waarp.openr66.dao.database.postgreSQL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.ClassRule;
import org.testcontainers.containers.PostgreSQLContainer;
import org.waarp.openr66.dao.database.DBTransferDAO;
import org.waarp.openr66.dao.database.DBTransferDAOIT;
import org.waarp.openr66.dao.database.PostgreSQLTransferDAO;
import org.waarp.openr66.dao.exception.DAOException;

public class DBTransferPostgreSQLDAOIT extends DBTransferDAOIT {

    private String createScript = "postgresql/create.sql";
    private String populateScript = "postgresql/populate.sql";
    private String cleanScript = "postgresql/clean.sql";

    @ClassRule
    public static PostgreSQLContainer db = new PostgreSQLContainer();

    @Override
    public DBTransferDAO getDAO(Connection con) throws DAOException {
        return new PostgreSQLTransferDAO(con);
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

