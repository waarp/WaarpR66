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
import org.waarp.openr66.dao.RuleDAO;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.database.DBRuleDAO;
import org.waarp.openr66.pojo.Rule;

public abstract class DBRuleDAOIT {

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
        RuleDAO dao = factory.getRuleDAO();
        dao.deleteAll();

        ResultSet res = con.createStatement()
            .executeQuery("SELECT * FROM RULES");
        assertEquals(false, res.next());
    }

    @Test
    public void testDelete() throws Exception {
        RuleDAO dao = factory.getRuleDAO();
        dao.delete(new Rule("default", 1));

        ResultSet res = con.createStatement()
            .executeQuery("SELECT * FROM RULES where idrule = 'default'");
        assertEquals(false, res.next());
    }

    @Test
    public void testGetAll() throws Exception {
        RuleDAO dao = factory.getRuleDAO();
        assertEquals(3, dao.getAll().size());
    }

    @Test
    public void testSelect() throws Exception {
        RuleDAO dao = factory.getRuleDAO();
        Rule rule = dao.select("dummy");
        Rule rule2 = dao.select("ghost");

        assertEquals("dummy", rule.getName());
        assertEquals(1, rule.getMode());
        assertEquals(3, rule.getHostids().size());
        assertEquals("/in", rule.getRecvPath());
        assertEquals("/out", rule.getSendPath());
        assertEquals("/arch", rule.getArchivePath());
        assertEquals("/work", rule.getWorkPath());
        assertEquals(0, rule.getRPreTasks().size());
        assertEquals(1, rule.getRPostTasks().size());
        assertEquals(2, rule.getRErrorTasks().size());
        assertEquals(3, rule.getSPreTasks().size());
        assertEquals(0, rule.getSPostTasks().size());
        assertEquals(0, rule.getSErrorTasks().size());
        assertEquals(42, rule.getUpdatedInfo());
        assertEquals(null, rule2);
    }

    @Test
    public void testExist() throws Exception {
        RuleDAO dao = factory.getRuleDAO();
        assertEquals(true, dao.exist("dummy"));
        assertEquals(false, dao.exist("ghost"));
    }


    @Test
    public void testInsert() throws Exception {
        RuleDAO dao = factory.getRuleDAO();
        dao.insert(new Rule("chacha", 2));

        ResultSet res = con.createStatement()
            .executeQuery("SELECT COUNT(1) as count FROM RULES");
        res.next();
        assertEquals(4, res.getInt("count"));

        ResultSet res2 = con.createStatement()
            .executeQuery("SELECT * FROM RULES WHERE idrule = 'chacha'");
        res2.next();
        assertEquals("chacha", res2.getString("idrule"));
        assertEquals(2, res2.getInt("modetrans"));
        assertEquals("<hostids></hostids>", res2.getString("hostids"));
        assertEquals("", res2.getString("recvpath"));
        assertEquals("", res2.getString("sendpath"));
        assertEquals("", res2.getString("archivepath"));
        assertEquals("", res2.getString("workpath"));
        assertEquals("<tasks></tasks>", res2.getString("rpretasks"));
        assertEquals("<tasks></tasks>", res2.getString("rposttasks"));
        assertEquals("<tasks></tasks>", res2.getString("rerrortasks"));
        assertEquals("<tasks></tasks>", res2.getString("spretasks"));
        assertEquals("<tasks></tasks>", res2.getString("sposttasks"));
        assertEquals("<tasks></tasks>", res2.getString("serrortasks"));
        assertEquals(0, res2.getInt("updatedInfo"));
    }

    @Test
    public void testUpdate() throws Exception {
        RuleDAO dao = factory.getRuleDAO();
        dao.update(new Rule("dummy", 2));

        ResultSet res = con.createStatement()
            .executeQuery("SELECT * FROM RULES WHERE idrule = 'dummy'");
        res.next();
        assertEquals("dummy", res.getString("idrule"));
        assertEquals(2, res.getInt("modetrans"));
        assertEquals("<hostids></hostids>", res.getString("hostids"));
        assertEquals("", res.getString("recvpath"));
        assertEquals("", res.getString("sendpath"));
        assertEquals("", res.getString("archivepath"));
        assertEquals("", res.getString("workpath"));
        assertEquals("<tasks></tasks>", res.getString("rpretasks"));
        assertEquals("<tasks></tasks>", res.getString("rposttasks"));
        assertEquals("<tasks></tasks>", res.getString("rerrortasks"));
        assertEquals("<tasks></tasks>", res.getString("spretasks"));
        assertEquals("<tasks></tasks>", res.getString("sposttasks"));
        assertEquals("<tasks></tasks>", res.getString("serrortasks"));
        assertEquals(0, res.getInt("updatedInfo"));
    } 


    @Test
    public void testFind() throws Exception {
        ArrayList<Filter> map = new ArrayList<Filter>();
        map.add(new Filter(DBRuleDAO.MODE_TRANS_FIELD, "=", 1));

        RuleDAO dao = factory.getRuleDAO();
        assertEquals(2, dao.find(map).size());
    } 
}

