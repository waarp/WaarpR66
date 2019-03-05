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
import org.waarp.openr66.dao.HostDAO;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.database.DBHostDAO;
import org.waarp.openr66.pojo.Host;

public abstract class DBHostDAOIT {

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
            fail(e.toString());
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
            HostDAO dao = new DBHostDAO(getConnection());
            dao.deleteAll();

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM HOSTS");
            assertEquals(false, res.next());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testDelete() {
        try {
            HostDAO dao = new DBHostDAO(getConnection());
            dao.delete(new Host("server1", "", 666, null, false, false));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM HOSTS where hostid = 'server1'");
            assertEquals(false, res.next());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testGetAll() {
        try {
            HostDAO dao = new DBHostDAO(getConnection());
            assertEquals(3, dao.getAll().size());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSelect() {
        try {
            HostDAO dao = new DBHostDAO(getConnection());
            Host host = dao.select("server1");
            Host host2 = dao.select("ghost");

            assertEquals("server1", host.getHostid());
            assertEquals("127.0.0.1", host.getAddress());
            assertEquals(6666, host.getPort());
            //HostKey is tested in Insert and Update
            assertEquals(true, host.isSSL());
            assertEquals(true, host.isClient());
            assertEquals(true, host.isProxified());
            assertEquals(false, host.isAdmin());
            assertEquals(false, host.isActive());
            assertEquals(42, host.getUpdatedInfo());

            assertEquals(null, host2);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testExist() {
        try {
            HostDAO dao = new DBHostDAO(getConnection());
            assertEquals(true, dao.exist("server1"));
            assertEquals(false, dao.exist("ghost"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }


    @Test
    public void testInsert() {
        try {
            HostDAO dao = new DBHostDAO(getConnection());
            dao.insert(new Host("chacha", "address", 666, "aaa".getBytes("utf-8"), false, false));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT COUNT(1) as count FROM HOSTS");
            res.next();
            assertEquals(4, res.getInt("count"));

            ResultSet res2 = con.createStatement()
                .executeQuery("SELECT * FROM HOSTS WHERE hostid = 'chacha'");
            res2.next();
            assertEquals("chacha", res2.getString("hostid"));
            assertEquals("address", res2.getString("address"));
            assertEquals(666, res2.getInt("port"));
            assertArrayEquals("aaa".getBytes("utf-8"), res2.getBytes("hostkey"));
            assertEquals(false, res2.getBoolean("isssl"));
            assertEquals(false, res2.getBoolean("isclient"));
            assertEquals(false, res2.getBoolean("isproxified"));
            assertEquals(true, res2.getBoolean("adminrole"));
            assertEquals(true, res2.getBoolean("isactive"));
            assertEquals(0, res2.getInt("updatedinfo"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testUpdate() {
        try {
            HostDAO dao = new DBHostDAO(getConnection());
            dao.update(new Host("server2", "address", 666, "password".getBytes("utf-8"), false, false));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM HOSTS WHERE hostid = 'server2'");
            res.next();
            assertEquals("server2", res.getString("hostid"));
            assertEquals("address", res.getString("address"));
            assertEquals(666, res.getInt("port"));
            assertArrayEquals("password".getBytes("utf-8"), res.getBytes("hostkey"));
            assertEquals(false, res.getBoolean("isssl"));
            assertEquals(false, res.getBoolean("isclient"));
            assertEquals(false, res.getBoolean("isproxified"));
            assertEquals(true, res.getBoolean("adminrole"));
            assertEquals(true, res.getBoolean("isactive"));
            assertEquals(0, res.getInt("updatedinfo"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }


    @Test
    public void testFind() {
        ArrayList<Filter> map = new ArrayList<Filter>();
        map.add(new Filter(DBHostDAO.ADDRESS_FIELD, "=", "127.0.0.1"));
        try {
            HostDAO dao = new DBHostDAO(getConnection());
            assertEquals(2, dao.find(map).size());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}

