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
import org.waarp.openr66.dao.BusinessDAO;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.database.DBBusinessDAO;
import org.waarp.openr66.pojo.Business;

public abstract class DBBusinessDAOIT {

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
        factory.close();
        con.close();
    }

    @Test
    public void testDeleteAll() throws Exception {
        BusinessDAO dao = factory.getBusinessDAO();
        dao.deleteAll();

        ResultSet res = con.createStatement()
            .executeQuery("SELECT * FROM HOSTCONFIG");
        assertEquals(false, res.next());
    }

    @Test
    public void testDelete() throws Exception {
        BusinessDAO dao = factory.getBusinessDAO();
        dao.delete(new Business("server1", "", "", "", ""));

        ResultSet res = con.createStatement()
            .executeQuery("SELECT * FROM HOSTCONFIG where hostid = 'server1'");
        assertEquals(false, res.next());
    }

    @Test
    public void testGetAll() throws Exception {
        BusinessDAO dao = factory.getBusinessDAO();

        assertEquals(5, dao.getAll().size());
    }

    @Test
    public void testSelect() throws Exception {
        BusinessDAO dao = factory.getBusinessDAO();
        Business business = dao.select("server1");
        Business business2 = dao.select("ghost");

        assertEquals("joyaux", business.getBusiness());
        assertEquals("marchand", business.getRoles());
        assertEquals("le borgne", business.getAliases());
        assertEquals("misc", business.getOthers());
        assertEquals(17, business.getUpdatedInfo());
        assertEquals(null, business2);
    }

    @Test
    public void testExist() throws Exception {
        BusinessDAO dao = factory.getBusinessDAO();

        assertEquals(true, dao.exist("server1"));
        assertEquals(false, dao.exist("ghost"));
    }


    @Test
    public void testInsert() throws Exception {
        BusinessDAO dao = factory.getBusinessDAO();
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
    }

    @Test
    public void testUpdate() throws Exception {
        BusinessDAO dao = factory.getBusinessDAO();
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
    } 


    @Test
    public void testFind() throws Exception {
        ArrayList<Filter> map = new ArrayList<Filter>();
        map.add(new Filter(DBBusinessDAO.BUSINESS_FIELD, "=", "ba"));

        BusinessDAO dao = factory.getBusinessDAO();
        assertEquals(2, dao.find(map).size());
    } 
}

