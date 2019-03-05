package org.waarp.openr66.dao.database.mariaDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Rule;
import org.testcontainers.containers.MariaDBContainer;
import org.waarp.openr66.dao.database.DBRuleDAOIT;

public class DBRuleMariaDBDAOIT extends DBRuleDAOIT {

    private String createScript = "createMySQL.sql";
    private String populateScript = "populateMySQL.sql";
    private String cleanScript = "cleanMySQL.sql";

    @Rule
    public MariaDBContainer db = new MariaDBContainer();

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

