package org.waarp.openr66.dao.database.postgreSQL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.ClassRule;
import org.testcontainers.containers.PostgreSQLContainer;
import org.waarp.openr66.dao.database.DBRuleDAOIT;

public class DBRulePostgreSQLDAOIT extends DBRuleDAOIT {

    private String createScript = "createPG.sql";
    private String populateScript = "populatePG.sql";
    private String cleanScript = "cleanPG.sql";

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

