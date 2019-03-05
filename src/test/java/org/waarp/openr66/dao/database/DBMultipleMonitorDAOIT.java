package org.waarp.openr66.dao.database;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static  org.junit.Assert.*;

import org.waarp.openr66.dao.DAOFactory;
import org.waarp.openr66.dao.MultipleMonitorDAO;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.database.DBMultipleMonitorDAO;
import org.waarp.openr66.pojo.MultipleMonitor;

public abstract class DBMultipleMonitorDAOIT {

    private Connection con;

    public abstract Connection getConnection() throws SQLException;
    public abstract void initDB() throws SQLException;
    public abstract void cleanDB() throws SQLException;

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
            con = getConnection();
            initDB();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @After
    public void wrapUp() {
        try {
            cleanDB();
            con.close();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testDeleteAll() {
        try {
            MultipleMonitorDAO dao = new DBMultipleMonitorDAO(getConnection());
            dao.deleteAll();

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM MULTIPLEMONITOR");
            assertEquals(false, res.next());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testDelete() {
        try {
            MultipleMonitorDAO dao = new DBMultipleMonitorDAO(getConnection());
            dao.delete(new MultipleMonitor("server1", 0, 0, 0));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM MULTIPLEMONITOR where hostid = 'server1'");
            assertEquals(false, res.next());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testGetAll() {
        try {
            MultipleMonitorDAO dao = new DBMultipleMonitorDAO(getConnection());
            assertEquals(4, dao.getAll().size());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSelect() {
        try {
            MultipleMonitorDAO dao = new DBMultipleMonitorDAO(getConnection());
            MultipleMonitor multiple = dao.select("server1");
            MultipleMonitor multiple2 = dao.select("ghost");

            assertEquals("server1", multiple.getHostid());
            assertEquals(11, multiple.getCountConfig());
            assertEquals(29, multiple.getCountRule());
            assertEquals(18, multiple.getCountHost());
            assertEquals(null, multiple2);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testExist() {
        try {
            MultipleMonitorDAO dao = new DBMultipleMonitorDAO(getConnection());
            assertEquals(true, dao.exist("server1"));
            assertEquals(false, dao.exist("ghost"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }


    @Test
    public void testInsert() {
        try {
            MultipleMonitorDAO dao = new DBMultipleMonitorDAO(getConnection());
            dao.insert(new MultipleMonitor("chacha", 31, 19, 98));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT COUNT(1) as count FROM MULTIPLEMONITOR");
            res.next();
            assertEquals(5, res.getInt("count"));

            ResultSet res2 = con.createStatement()
                .executeQuery("SELECT * FROM MULTIPLEMONITOR WHERE hostid = 'chacha'");
            res2.next();
            assertEquals("chacha", res2.getString("hostid"));
            assertEquals(98, res2.getInt("countRule"));
            assertEquals(19, res2.getInt("countHost"));
            assertEquals(31, res2.getInt("countConfig"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testUpdate() {
        try {
            MultipleMonitorDAO dao = new DBMultipleMonitorDAO(getConnection());
            dao.update(new MultipleMonitor("server2", 31, 19, 98));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM MULTIPLEMONITOR WHERE hostid = 'server2'");
            res.next();
            assertEquals("server2", res.getString("hostid"));
            assertEquals(98, res.getInt("countRule"));
            assertEquals(19, res.getInt("countHost"));
            assertEquals(31, res.getInt("countConfig"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }


    @Test
    public void testFind() {
        ArrayList<Filter> map = new ArrayList<Filter>();
        map.add(new Filter(DBMultipleMonitorDAO.COUNT_CONFIG_FIELD, "=" , 0));
        try {
            MultipleMonitorDAO dao = new DBMultipleMonitorDAO(getConnection());
            assertEquals(2, dao.find(map).size());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}

