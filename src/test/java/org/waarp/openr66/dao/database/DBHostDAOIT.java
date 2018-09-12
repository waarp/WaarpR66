package org.waarp.openr66.dao.database.test;

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

import static org.mockito.Mockito.*;

import org.waarp.common.database.ConnectionFactory;
import org.waarp.openr66.configuration.Configuration;
import org.waarp.openr66.dao.DAOFactory;
import org.waarp.openr66.dao.HostDAO;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.database.DBHostDAO;
import org.waarp.openr66.pojo.Host;

public abstract class DBHostDAOIT {

    private DAOFactory factory;
    private Connection con;

    public abstract Connection getConnection() throws SQLException;
    public abstract void initDB() throws Exception;
    public abstract void cleanDB() throws Exception;

    public void runScript(String script) throws Exception {
        ScriptRunner runner = new ScriptRunner(con, false, true); 
        URL url = Thread.currentThread().getContextClassLoader().getResource(script);
        runner.runScript(new BufferedReader(new FileReader(url.getPath())));
    }

    @Before
    public void setUp() throws Exception {
        //Force DAOFactory to use DBDAO
        Configuration.database.url = "";
        //Mock factory.getConnection()
        ConnectionFactory mockedFactory = mock(ConnectionFactory.class);
        Configuration.database.factory = mockedFactory;
        when(mockedFactory.getConnection()).thenReturn(getConnection());

        con = getConnection();
        factory = DAOFactory.getDAOFactory();
        initDB();
    }

    @After
    public void wrapUp() throws Exception {
        cleanDB();
        con.close();
        factory.close();
    }

    @Test
    public void testDeleteAll() throws Exception {
        HostDAO dao = factory.getHostDAO();
        dao.deleteAll();

        ResultSet res = con.createStatement()
            .executeQuery("SELECT * FROM HOSTS");
        assertEquals(false, res.next());
    }

    @Test
    public void testDelete() throws Exception {
        HostDAO dao = factory.getHostDAO();
        dao.delete(new Host("server1", "", 666, null, false, false));

        ResultSet res = con.createStatement()
            .executeQuery("SELECT * FROM HOSTS where hostid = 'server1'");
        assertEquals(false, res.next());
    }

    @Test
    public void testGetAll() throws Exception {
        HostDAO dao = factory.getHostDAO();
        assertEquals(3, dao.getAll().size());
    }

    @Test
    public void testSelect() throws Exception {
        HostDAO dao = factory.getHostDAO();
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
    }

    @Test
    public void testExist() throws Exception {
        HostDAO dao = factory.getHostDAO();
        assertEquals(true, dao.exist("server1"));
        assertEquals(false, dao.exist("ghost"));
    }


    @Test
    public void testInsert() throws Exception {
        HostDAO dao = factory.getHostDAO();
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
    }

    @Test
    public void testUpdate() throws Exception {
        HostDAO dao = factory.getHostDAO();
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
    } 


    @Test
    public void testFind() throws Exception {
        ArrayList<Filter> map = new ArrayList<Filter>();
        map.add(new Filter(DBHostDAO.ADDRESS_FIELD, "=", "127.0.0.1"));

        HostDAO dao = factory.getHostDAO();
        assertEquals(2, dao.find(map).size());
    } 
}

