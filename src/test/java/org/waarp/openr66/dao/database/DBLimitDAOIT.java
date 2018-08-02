package org.waarp.openr66.dao.database.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import static  org.junit.Assert.*;

import org.testcontainers.containers.PostgreSQLContainer;

import org.waarp.openr66.dao.DAOFactory;
import org.waarp.openr66.dao.LimitDAO;
import org.waarp.openr66.dao.database.DBLimitDAO;
import org.waarp.openr66.pojo.Limit;

public class DBLimitDAOIT {

    @Rule
    public PostgreSQLContainer db = new PostgreSQLContainer();

    private DAOFactory factory;
    private Connection con;

    public void runScript(String script) {
        try {
            ScriptRunner runner = new ScriptRunner(con, false, true); 
            URL url = Thread.currentThread().getContextClassLoader().getResource(script);
            runner.runScript(new BufferedReader(new FileReader(url.getPath())));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Before
    public void setUp() {
        try {
            //InitDatabase
            con = DriverManager.getConnection(
                    db.getJdbcUrl(),
                    db.getUsername(),
                    db.getPassword());
            runScript("initDB.sql"); 
            //Create factory 
            factory = DAOFactory.getDAOFactory(DriverManager.getConnection(
                        db.getJdbcUrl(),
                        db.getUsername(),
                        db.getPassword()));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @After
    public void wrapUp() {
        try {
            runScript("wrapDB.sql");
            con.close();
            //factory.close();
        } catch (Exception e) {
            fail(e.getMessage());
        } 
    }

    @Test
    public void testDeleteAll() {
        try {
            LimitDAO dao = factory.getLimitDAO();
            dao.deleteAll();

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM CONFIGURATION");
            assertEquals(false, res.next());
        } catch (Exception e) {
            fail(e.getMessage());
        } 
    }

    @Test
    public void testDelete() {
        try {
            LimitDAO dao = factory.getLimitDAO();
            dao.delete(new Limit("server1", 0l));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM CONFIGURATION where hostid = 'server1'");
            assertEquals(false, res.next());
        } catch (Exception e) {
            fail(e.getMessage());
        } 
    }

    @Test
    public void testGetAll() {
        try {
            LimitDAO dao = factory.getLimitDAO();
            assertEquals(3, dao.getAll().size());
        } catch (Exception e) {
            fail(e.getMessage());
        } 
    }

    @Test
    public void testSelect() {
        try {
            LimitDAO dao = factory.getLimitDAO();
            Limit limit = dao.select("server1");
            Limit limit2 = dao.select("ghost");

            assertEquals("server1", limit.getHostid());
            assertEquals(1, limit.getReadGlobalLimit());
            assertEquals(2, limit.getWriteGlobalLimit());
            assertEquals(3, limit.getReadSessionLimit());
            assertEquals(4, limit.getWriteSessionLimit());
            assertEquals(5, limit.getDelayLimit());
            assertEquals(42, limit.getUpdatedInfo());
            assertEquals(null, limit2);
        } catch (Exception e) {
            fail(e.getMessage());
        } 
    }

    @Test
    public void testExist() {
        try {
            LimitDAO dao = factory.getLimitDAO();
            assertEquals(true, dao.exist("server1"));
            assertEquals(false, dao.exist("ghost"));
        } catch (Exception e) {
            fail(e.getMessage());
        } 
    }


    @Test
    public void testInsert() {
        try {
            LimitDAO dao = factory.getLimitDAO();
            dao.insert(new Limit("chacha", 4l, 1l, 5l, 13l, 12, -18));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT COUNT(1) FROM CONFIGURATION");
            res.next();
            assertEquals(4, res.getInt("count"));

            ResultSet res2 = con.createStatement()
                .executeQuery("SELECT * FROM CONFIGURATION WHERE hostid = 'chacha'");
            res2.next();
            assertEquals("chacha", res2.getString("hostid"));
            assertEquals(4, res2.getLong("delaylimit"));
            assertEquals(1, res2.getLong("readGlobalLimit"));
            assertEquals(5, res2.getLong("writeGlobalLimit"));
            assertEquals(13, res2.getLong("readSessionLimit"));
            assertEquals(12, res2.getLong("writeSessionLimit"));
            assertEquals(-18, res2.getInt("updatedInfo"));
        } catch (Exception e) {
            fail(e.getMessage());
        } 
    }

    @Test
    public void testUpdate() {
        try {
            LimitDAO dao = factory.getLimitDAO();
            dao.update(new Limit("server2", 4l, 1l, 5l, 13l, 12l, 18));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM CONFIGURATION WHERE hostid = 'server2'");
            res.next();
            assertEquals("server2", res.getString("hostid"));
            assertEquals(4, res.getLong("delaylimit"));
            assertEquals(1, res.getLong("readGlobalLimit"));
            assertEquals(5, res.getLong("writeGlobalLimit"));
            assertEquals(13, res.getLong("readSessionLimit"));
            assertEquals(12, res.getLong("writeSessionLimit"));
            assertEquals(18, res.getInt("updatedInfo"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    } 


    @Test
    public void testFind() {
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put(DBLimitDAO.READ_SESSION_LIMIT_FIELD, 3);
        try {
            LimitDAO dao = factory.getLimitDAO();
            assertEquals(2, dao.find(map).size());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    } 
}

