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
import org.waarp.openr66.dao.MultipleMonitorDAO;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.database.DBMultipleMonitorDAO;
import org.waarp.openr66.pojo.MultipleMonitor;

public abstract class DBMultipleMonitorDAOIT {

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
        MultipleMonitorDAO dao = factory.getMultipleMonitorDAO();
        dao.deleteAll();

        ResultSet res = con.createStatement()
            .executeQuery("SELECT * FROM MULTIPLEMONITOR");
        assertEquals(false, res.next());
    }

    @Test
    public void testDelete() throws Exception {
        MultipleMonitorDAO dao = factory.getMultipleMonitorDAO();
        dao.delete(new MultipleMonitor("server1", 0, 0, 0));

        ResultSet res = con.createStatement()
            .executeQuery("SELECT * FROM MULTIPLEMONITOR where hostid = 'server1'");
        assertEquals(false, res.next());
    }

    @Test
    public void testGetAll() throws Exception {
        MultipleMonitorDAO dao = factory.getMultipleMonitorDAO();
        assertEquals(4, dao.getAll().size());
    }

    @Test
    public void testSelect() throws Exception {
        MultipleMonitorDAO dao = factory.getMultipleMonitorDAO();
        MultipleMonitor multiple = dao.select("server1");
        MultipleMonitor multiple2 = dao.select("ghost");

        assertEquals("server1", multiple.getHostid());
        assertEquals(11, multiple.getCountConfig());
        assertEquals(29, multiple.getCountRule());
        assertEquals(18, multiple.getCountHost());
        assertEquals(null, multiple2);
    }

    @Test
    public void testExist() throws Exception {
        MultipleMonitorDAO dao = factory.getMultipleMonitorDAO();
        assertEquals(true, dao.exist("server1"));
        assertEquals(false, dao.exist("ghost"));
    }


    @Test
    public void testInsert() throws Exception {
        MultipleMonitorDAO dao = factory.getMultipleMonitorDAO();
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
    }

    @Test
    public void testUpdate() throws Exception {
        MultipleMonitorDAO dao = factory.getMultipleMonitorDAO();
        dao.update(new MultipleMonitor("server2", 31, 19, 98));

        ResultSet res = con.createStatement()
            .executeQuery("SELECT * FROM MULTIPLEMONITOR WHERE hostid = 'server2'");
        res.next();
        assertEquals("server2", res.getString("hostid"));
        assertEquals(98, res.getInt("countRule"));
        assertEquals(19, res.getInt("countHost"));
        assertEquals(31, res.getInt("countConfig"));
    } 


    @Test
    public void testFind() throws Exception {
        ArrayList<Filter> map = new ArrayList<Filter>();
        map.add(new Filter(DBMultipleMonitorDAO.COUNT_CONFIG_FIELD, "=" , 0));

        MultipleMonitorDAO dao = factory.getMultipleMonitorDAO();
        assertEquals(2, dao.find(map).size());
    } 
}

