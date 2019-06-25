package org.waarp.openr66.dao.database.postgreSQL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.ClassRule;
import org.testcontainers.containers.PostgreSQLContainer;
import org.waarp.openr66.dao.database.DBLimitDAOIT;

public class DBLimitPostgreSQLDAOIT extends DBLimitDAOIT {

    private String createScript = "postgresql/create.sql";
    private String populateScript = "postgresql/populate.sql";
    private String cleanScript = "postgresql/clean.sql";

    @ClassRule
    public static PostgreSQLContainer db = new PostgreSQLContainer();

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

