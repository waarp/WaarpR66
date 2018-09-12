package org.waarp.openr66.dao.database.mySQL.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Rule;
import org.testcontainers.containers.MySQLContainer;
import org.waarp.openr66.dao.database.test.DBRuleDAOIT;

public class DBRuleMySQLDAOIT extends DBRuleDAOIT {

    private String createScript = "createMySQL.sql";
    private String populateScript = "populateMySQL.sql";
    private String cleanScript = "cleanMySQL.sql";

    @Rule
    public MySQLContainer db = new MySQLContainer();

    @Override
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                db.getJdbcUrl(),
                db.getUsername(),
                db.getPassword());
    }

    @Override
    public void initDB() throws Exception {
        runScript(createScript); 
        runScript(populateScript); 
    }

    @Override
    public void cleanDB() throws Exception {
        runScript(cleanScript);
    }
}

