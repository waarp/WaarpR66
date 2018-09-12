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
import org.waarp.openr66.dao.LimitDAO;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.database.DBLimitDAO;
import org.waarp.openr66.pojo.Limit;

public abstract class DBLimitDAOIT {

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
        LimitDAO dao = factory.getLimitDAO();
        dao.deleteAll();

        ResultSet res = con.createStatement()
            .executeQuery("SELECT * FROM CONFIGURATION");
        assertEquals(false, res.next());
    }

    @Test
    public void testDelete() throws Exception {
        LimitDAO dao = factory.getLimitDAO();
        dao.delete(new Limit("server1", 0l));

        ResultSet res = con.createStatement()
            .executeQuery("SELECT * FROM CONFIGURATION where hostid = 'server1'");
        assertEquals(false, res.next());
    }

    @Test
    public void testGetAll() throws Exception {
        LimitDAO dao = factory.getLimitDAO();
        assertEquals(3, dao.getAll().size());
    }

    @Test
    public void testSelect() throws Exception {
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
    }

    @Test
    public void testExist() throws Exception {
        LimitDAO dao = factory.getLimitDAO();
        assertEquals(true, dao.exist("server1"));
        assertEquals(false, dao.exist("ghost"));
    }


    @Test
    public void testInsert() throws Exception {
        LimitDAO dao = factory.getLimitDAO();
        dao.insert(new Limit("chacha", 4l, 1l, 5l, 13l, 12, -18));

        ResultSet res = con.createStatement()
            .executeQuery("SELECT COUNT(1) as count FROM CONFIGURATION");
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
    }

    @Test
    public void testUpdate() throws Exception {
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
    } 


    @Test
    public void testFind() throws Exception {
        ArrayList<Filter> map = new ArrayList<Filter>();
        map.add(new Filter(DBLimitDAO.READ_SESSION_LIMIT_FIELD, ">", 2));

        LimitDAO dao = factory.getLimitDAO();
        assertEquals(2, dao.find(map).size());
    } 
}

