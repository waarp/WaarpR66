package org.waarp.openr66.dao.database;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static  org.junit.Assert.*;

import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.dao.DAOFactory;
import org.waarp.openr66.dao.TransferDAO;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.database.DBTransferDAO;
import org.waarp.openr66.pojo.Transfer;
import org.waarp.openr66.pojo.UpdatedInfo;

public abstract class DBTransferDAOIT {

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
            TransferDAO dao = new DBTransferDAO(getConnection());
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
            TransferDAO dao = new DBTransferDAO(getConnection());
            dao.delete(new Transfer(0l, "", 1, "", "", "", false, 0, false, "",
                    "", "", "", Transfer.TASKSTEP.NOTASK,
                    Transfer.TASKSTEP.NOTASK, 0, ErrorCode.Unknown,
                    ErrorCode.Unknown , 0, null, null));

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
            TransferDAO dao = new DBTransferDAO(getConnection());
            assertEquals(4, dao.getAll().size());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSelect() {
        try {
            TransferDAO dao = new DBTransferDAO(getConnection());
            Transfer transfer = dao.select(0l, "server1", "server2");
            Transfer transfer2 = dao.select(1l, "server1", "server2");

            assertEquals(0, transfer.getId());
            assertEquals(null, transfer2);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testExist() {
        try {
            TransferDAO dao = new DBTransferDAO(getConnection());
            assertEquals(true, dao.exist(0l, "server1", "server2"));
            assertEquals(false, dao.exist(1l, "server1", "server2"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }


    @Test
    public void testInsert() {
        try {
            TransferDAO dao = new DBTransferDAO(getConnection());
            Transfer transfer = new Transfer("server2", "rule", 1, false,
                    "file", "info", 3);
            transfer.setStart(new Timestamp(1112242l));
            transfer.setStop(new Timestamp(122l));
            dao.insert(transfer);

            ResultSet res = con.createStatement()
                .executeQuery("SELECT COUNT(1) as count FROM RUNNER");
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
            TransferDAO dao = new DBTransferDAO(getConnection());
            dao.update(new Transfer(0l, "rule", 13, "test", "testOrig",
                    "testInfo", true, 42, true, "owner", "requester",
                    "requested", "transferInfo", Transfer.TASKSTEP.ERRORTASK,
                    Transfer.TASKSTEP.TRANSFERTASK, 27, ErrorCode.Unknown,
                    ErrorCode.Unknown, 64, new Timestamp(192l),
                    new Timestamp(1511l), UpdatedInfo.UNKNOWN));

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
            assertEquals(ErrorCode.Unknown.code, res.getString("stepstatus"));
            assertEquals(ErrorCode.Unknown.code, res.getString("infostatus"));
            assertEquals(64, res.getInt("rank"));
            assertEquals(new Timestamp(192l), res.getTimestamp("starttrans"));
            assertEquals(new Timestamp(1511l), res.getTimestamp("stoptrans"));
            assertEquals(UpdatedInfo.UNKNOWN.ordinal(), res.getInt("updatedInfo"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }


    @Test
    public void testFind() {
        ArrayList<Filter> map = new ArrayList<Filter>();
        map.add(new Filter(DBTransferDAO.ID_RULE_FIELD, "=", "tintin"));
        map.add(new Filter(DBTransferDAO.OWNER_REQUEST_FIELD,"=", "server1"));
        try {
            TransferDAO dao = new DBTransferDAO(getConnection());
            assertEquals(2, dao.find(map).size());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}

