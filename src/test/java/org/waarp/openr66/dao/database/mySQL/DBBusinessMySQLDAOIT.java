package org.waarp.openr66.dao.database.mySQL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.ClassRule;
import org.testcontainers.containers.MySQLContainer;
import org.waarp.openr66.dao.database.DBBusinessDAOIT;

public class DBBusinessMySQLDAOIT extends DBBusinessDAOIT {

    private String createScript = "mysql/create.sql";
    private String populateScript = "mysql/populate.sql";
    private String cleanScript = "mysql/clean.sql";

    @ClassRule
    public static MySQLContainer db = new MySQLContainer();

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

