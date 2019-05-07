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

import org.waarp.openr66.dao.RuleDAO;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.pojo.Rule;
import org.waarp.openr66.pojo.UpdatedInfo;

public abstract class DBRuleDAOIT {

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
            RuleDAO dao = new DBRuleDAO(getConnection());
            dao.deleteAll();

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM rules");
            assertEquals(false, res.next());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testDelete() {
        try {
            RuleDAO dao = new DBRuleDAO(getConnection());
            dao.delete(new Rule("default", 1));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM rules where idrule = 'default'");
            assertEquals(false, res.next());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testGetAll() {
        try {
            RuleDAO dao = new DBRuleDAO(getConnection());
            assertEquals(3, dao.getAll().size());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSelect() {
        try {
            RuleDAO dao = new DBRuleDAO(getConnection());
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
            assertEquals(UpdatedInfo.UNKNOWN, rule.getUpdatedInfo());
            assertEquals(null, rule2);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testExist() {
        try {
            RuleDAO dao = new DBRuleDAO(getConnection());
            assertEquals(true, dao.exist("dummy"));
            assertEquals(false, dao.exist("ghost"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }


    @Test
    public void testInsert() {
        try {
            RuleDAO dao = new DBRuleDAO(getConnection());
            dao.insert(new Rule("chacha", 2));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT COUNT(1) as count FROM rules");
            res.next();
            assertEquals(4, res.getInt("count"));

            ResultSet res2 = con.createStatement()
                .executeQuery("SELECT * FROM rules WHERE idrule = 'chacha'");
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
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testUpdate() {
        try {
            RuleDAO dao = new DBRuleDAO(getConnection());
            dao.update(new Rule("dummy", 2));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM rules WHERE idrule = 'dummy'");
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
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }


    @Test
    public void testFind() {
        ArrayList<Filter> map = new ArrayList<Filter>();
        map.add(new Filter(DBRuleDAO.MODE_TRANS_FIELD, "=", 1));
        try {
            RuleDAO dao = new DBRuleDAO(getConnection());
            assertEquals(2, dao.find(map).size());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}

