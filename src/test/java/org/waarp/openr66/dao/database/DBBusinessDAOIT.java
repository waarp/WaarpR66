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

import org.waarp.openr66.dao.BusinessDAO; 
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.pojo.Business;

public abstract class DBBusinessDAOIT {

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
            BusinessDAO dao = new DBBusinessDAO(getConnection());
            dao.deleteAll();

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM HOSTCONFIG");
            assertEquals(false, res.next());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testDelete() {
        try {
            BusinessDAO dao = new DBBusinessDAO(getConnection());
            dao.delete(new Business("server1", "", "", "", ""));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM HOSTCONFIG where hostid = 'server1'");
            assertEquals(false, res.next());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testGetAll() {
        try {
            BusinessDAO dao = new DBBusinessDAO(getConnection());
            assertEquals(5, dao.getAll().size());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSelect() {
        try {
            BusinessDAO dao = new DBBusinessDAO(getConnection());
            Business business = dao.select("server1");
            Business business2 = dao.select("ghost");

            assertEquals("joyaux", business.getBusiness());
            assertEquals("marchand", business.getRoles());
            assertEquals("le borgne", business.getAliases());
            assertEquals("misc", business.getOthers());
            assertEquals(17, business.getUpdatedInfo());
            assertEquals(null, business2);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testExist() {
        try {
            BusinessDAO dao = new DBBusinessDAO(getConnection());
            assertEquals(true, dao.exist("server1"));
            assertEquals(false, dao.exist("ghost"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }


    @Test
    public void testInsert() {
        try {
            BusinessDAO dao = new DBBusinessDAO(getConnection());
            dao.insert(new Business("chacha", "lolo", "lala", "minou", "ect", -18));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT COUNT(1) as count FROM HOSTCONFIG");
            res.next();
            assertEquals(6, res.getInt("count"));

            ResultSet res2 = con.createStatement()
                .executeQuery("SELECT * FROM HOSTCONFIG WHERE hostid = 'chacha'");
            res2.next();
            assertEquals("chacha", res2.getString("hostid"));
            assertEquals("lolo", res2.getString("business"));
            assertEquals("lala", res2.getString("roles"));
            assertEquals("minou", res2.getString("aliases"));
            assertEquals("ect", res2.getString("others"));
            assertEquals(-18, res2.getInt("updatedInfo"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testUpdate() {
        try {
            BusinessDAO dao = new DBBusinessDAO(getConnection());
            dao.update(new Business("server2", "lolo", "lala", "minou", "ect", 18));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM HOSTCONFIG WHERE hostid = 'server2'");
            res.next();
            assertEquals("server2", res.getString("hostid"));
            assertEquals("lolo", res.getString("business"));
            assertEquals("lala", res.getString("roles"));
            assertEquals("minou", res.getString("aliases"));
            assertEquals("ect", res.getString("others"));
            assertEquals(18, res.getInt("updatedInfo"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }


    @Test
    public void testFind() {
        ArrayList<Filter> map = new ArrayList<Filter>();
        map.add(new Filter(DBBusinessDAO.BUSINESS_FIELD, "=", "ba"));
        try {
            BusinessDAO dao = new DBBusinessDAO(getConnection());
            assertEquals(2, dao.find(map).size());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}

