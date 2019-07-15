package org.waarp.openr66.dao.database.oracle;

import org.junit.ClassRule;
import org.testcontainers.containers.OracleContainer;
import org.waarp.openr66.dao.database.DBRuleDAOIT;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBRuleOracleDBDAOIT extends DBRuleDAOIT {

    private String createScript = "oracle/create.sql";
    private String populateScript = "oracle/populate.sql";
    private String cleanScript = "oracle/clean.sql";

    @ClassRule
    public static OracleContainer db = new OracleContainer("epiclabs/docker-oracle-xe-11g");

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

