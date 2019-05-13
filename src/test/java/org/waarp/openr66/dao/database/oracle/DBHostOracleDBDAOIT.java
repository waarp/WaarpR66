package org.waarp.openr66.dao.database.oracle;

import org.junit.ClassRule;
import org.testcontainers.containers.OracleContainer;
import org.waarp.openr66.dao.database.DBHostDAOIT;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBHostOracleDBDAOIT extends DBHostDAOIT {

    private String createScript = "createMySQL.sql";
    private String populateScript = "populateMySQL.sql";
    private String cleanScript = "cleanMySQL.sql";

    @ClassRule
    public static OracleContainer db = new OracleContainer("test");

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

