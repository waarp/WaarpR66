package org.waarp.openr66.dao.database.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import static  org.junit.Assert.*;

import org.testcontainers.containers.PostgreSQLContainer;

import org.waarp.openr66.dao.DAOFactory;
import org.waarp.openr66.dao.TransferDAO;
import org.waarp.openr66.dao.database.DBTransferDAO;
import org.waarp.openr66.pojo.Transfer;

public class DBTransferDAOIT {

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
            TransferDAO dao = factory.getTransferDAO();
            dao.deleteAll();

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM RUNNER");
            assertEquals(false, res.next());
        } catch (Exception e) {
            fail(e.getMessage());
        } 
    }

    @Test
    public void testDelete() {
        try {
            TransferDAO dao = factory.getTransferDAO();
            dao.delete(new Transfer(0l, "", 1, "", "", "", false, 0, false, "",
                        "", "", "", Transfer.TASKSTEP.NOTASK, 
                        Transfer.TASKSTEP.NOTASK, 0, "", "" , 0, null, null));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM RUNNER where specialid = 0");
            assertEquals(false, res.next());
        } catch (Exception e) {
            fail(e.getMessage());
        } 
    }

    @Test
    public void testGetAll() {
        try {
            TransferDAO dao = factory.getTransferDAO();
            assertEquals(4, dao.getAll().size());
        } catch (Exception e) {
            fail(e.getMessage());
        } 
    }

    @Test
    public void testSelect() {
        try {
            TransferDAO dao = factory.getTransferDAO();
            Transfer transfer = dao.select(0l);
            Transfer transfer2 = dao.select(1l);

            assertEquals(0, transfer.getId());
            assertEquals(null, transfer2);
        } catch (Exception e) {
            fail(e.getMessage());
        } 
    }

    @Test
    public void testExist() {
        try {
            TransferDAO dao = factory.getTransferDAO();
            assertEquals(true, dao.exist(0l));
            assertEquals(false, dao.exist(1));
        } catch (Exception e) {
            fail(e.getMessage());
        } 
    }


    @Test
    public void testInsert() {
        try {
            TransferDAO dao = factory.getTransferDAO();
            Transfer transfer = new Transfer("rule", 1, false, "file", "info", 3);
            transfer.setStart(new Timestamp(1112242l));
            transfer.setStop(new Timestamp(122l));
            dao.insert(transfer);

            ResultSet res = con.createStatement()
                .executeQuery("SELECT COUNT(1) FROM RUNNER");
            res.next();
            assertEquals(5, res.getInt("count"));

            ResultSet res2 = con.createStatement()
                .executeQuery("SELECT * FROM RUNNER WHERE idrule = 'rule'");
            res2.next();
            //assertEquals(0, res.getLong("id"));
            assertEquals("rule", res.getLong("idrule"));
            assertEquals(1, res.getInt("modetrans"));
            assertEquals("file", res.getString("filename"));
            assertEquals("file", res.getString("originalname"));
            assertEquals("info", res.getString("fileinfo"));
            assertEquals(false, res.getBoolean("ismoved"));
            assertEquals(3, res.getInt("blocksz"));
            assertEquals(19000, res.getInt(0));
        } catch (Exception e) {
            fail(e.getMessage());
        } 
    }

    @Test
    public void testUpdate() {
        try {
            TransferDAO dao = factory.getTransferDAO();
            dao.update(new Transfer(0l, "rule", 13, "test", "testOrig", 
                        "testInfo", true, 42, true, "owner", "requester", 
                        "requested", "transferInfo", Transfer.TASKSTEP.ERRORTASK, 
                        Transfer.TASKSTEP.TRANSFERTASK, 27, "ste", "inf",
                        64, new Timestamp(192l), new Timestamp(1511l), 19000));

            ResultSet res = con.createStatement()
                .executeQuery("SELECT * FROM RUNNER WHERE specialid = 0");
            res.next();
            assertEquals(0, res.getLong("specialid"));
            assertEquals("rule", res.getString("idrule"));
            assertEquals(13, res.getInt("modetrans"));
            assertEquals("test", res.getString("filename"));
            assertEquals("testOrig", res.getString("originalname"));
            assertEquals("testInfo", res.getString("fileinfo"));
            assertEquals(true, res.getBoolean("ismoved"));
            assertEquals(42, res.getInt("blocksz"));
            assertEquals(true, res.getBoolean("retrievemode"));
            assertEquals("owner", res.getString("ownerreq"));
            assertEquals("requester", res.getString("requester"));
            assertEquals("requested", res.getString("requested"));
            assertEquals(Transfer.TASKSTEP.ERRORTASK.ordinal(), res.getInt("globalstep"));
            assertEquals(Transfer.TASKSTEP.TRANSFERTASK.ordinal(), res.getInt("globallaststep"));
            assertEquals(27, res.getInt("step"));
            assertEquals("ste", res.getString("stepstatus"));
            assertEquals("inf", res.getString("infostatus"));
            assertEquals(64, res.getInt("rank"));
            assertEquals(new Timestamp(192l), res.getTimestamp("starttrans"));
            assertEquals(new Timestamp(1511l), res.getTimestamp("stoptrans"));
            assertEquals(19000, res.getInt("updatedInfo"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    } 


    @Test
    public void testFind() {
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put(DBTransferDAO.FILENAME_FIELD, "tintin");
        try {
            TransferDAO dao = factory.getTransferDAO();
            assertEquals(2, dao.find(map).size());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    } 
}

