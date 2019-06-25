package org.waarp.openr66.dao.database.mariaDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.ClassRule;
import org.testcontainers.containers.MariaDBContainer;
import org.waarp.openr66.dao.database.DBMultipleMonitorDAOIT;

public class DBMultipleMonitorMariaDBDAOIT extends DBMultipleMonitorDAOIT {

    private String createScript = "mysql/create.sql";
    private String populateScript = "mysql/populate.sql";
    private String cleanScript = "mysql/clean.sql";

    @ClassRule
    public static MariaDBContainer db = new MariaDBContainer();

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

