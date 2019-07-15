package org.waarp.openr66.dao.database;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.TransferDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Transfer;
import org.waarp.openr66.pojo.UpdatedInfo;
import org.waarp.openr66.protocol.configuration.Configuration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class DBTransferDAOIT {

    private Connection con;

    public abstract TransferDAO getDAO(Connection con) throws DAOException;
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
    public void testDeleteAll() throws Exception {
        TransferDAO dao = getDAO(getConnection());
        dao.deleteAll();

        ResultSet res = con.createStatement()
            .executeQuery("SELECT * FROM runner");
        assertEquals(false, res.next());
    }

    @Test
    public void testDelete() throws Exception {
        TransferDAO dao = getDAO(getConnection());
        dao.delete(new Transfer(0l, "", 1, "", "", "", false, 0, false,
                "server1", "server1", "server2", "",
                Transfer.TASKSTEP.NOTASK, Transfer.TASKSTEP.NOTASK, 0,
                ErrorCode.Unknown, ErrorCode.Unknown, 0, null, null));

        ResultSet res = con.createStatement()
            .executeQuery("SELECT * FROM runner where specialid = 0");
        assertEquals(false, res.next());
    }

    @Test
    public void testGetAll() throws Exception {
        TransferDAO dao = getDAO(getConnection());
        assertEquals(4, dao.getAll().size());
    }

    @Test
    public void testSelect() throws Exception {
        TransferDAO dao = getDAO(getConnection());
        Transfer transfer = dao.select(0l, "server1", "server2", "server1");
        Transfer transfer2 = dao.select(1l, "server1", "server2", "server1");

        assertEquals(0, transfer.getId());
        assertEquals(null, transfer2);
    }

    @Test
    public void testExist() throws Exception {
        TransferDAO dao = getDAO(getConnection());
        assertEquals(true, dao.exist(0l, "server1", "server2", "server1"));
        assertEquals(false, dao.exist(1l, "server1", "server2", "server1"));
    }

    @Test
    public void testInsert() throws Exception {
        TransferDAO dao = getDAO(getConnection());
        Transfer transfer = new Transfer("server2", "rule", 1, false,
                "file", "info", 3);
        // Requester and requested are setup manualy
        transfer.setRequester("dummy");
        transfer.setOwnerRequest("dummy");
        transfer.setStart(new Timestamp(1112242l));
        transfer.setStop(new Timestamp(122l));
        dao.insert(transfer);

        ResultSet res = con.createStatement()
            .executeQuery("SELECT COUNT(1) as count FROM runner");
        res.next();
        assertEquals(5, res.getInt("count"));

        ResultSet res2 = con.createStatement()
            .executeQuery("SELECT * FROM runner WHERE idrule = 'rule'");
        res2.next();
        assertEquals("rule", res2.getString("idrule"));
        assertEquals(1, res2.getInt("modetrans"));
        assertEquals("file", res2.getString("filename"));
        assertEquals("file", res2.getString("originalname"));
        assertEquals("info", res2.getString("fileinfo"));
        assertEquals(false, res2.getBoolean("ismoved"));
        assertEquals(3, res2.getInt("blocksz"));
    }

    @Test
    public void testUpdate() throws Exception {
        TransferDAO dao = getDAO(getConnection());

        dao.update(new Transfer(0l, "rule", 13, "test", "testOrig",
                "testInfo", true, 42, true, "server1", "server1",
                "server2", "transferInfo", Transfer.TASKSTEP.ERRORTASK,
                Transfer.TASKSTEP.TRANSFERTASK, 27, ErrorCode.CompleteOk,
                ErrorCode.Unknown, 64, new Timestamp(192l),
                new Timestamp(1511l), UpdatedInfo.TOSUBMIT));

        ResultSet res = con.createStatement()
            .executeQuery("SELECT * FROM runner WHERE specialid=0 and " +
                    "ownerreq='server1' and requester='server1' and " +
                    "requested='server2'");
        if (!res.next()) {
            fail("Result not found");
        }
        assertEquals(0, res.getLong("specialid"));
        assertEquals("rule", res.getString("idrule"));
        assertEquals(13, res.getInt("modetrans"));
        assertEquals("test", res.getString("filename"));
        assertEquals("testOrig", res.getString("originalname"));
        assertEquals("testInfo", res.getString("fileinfo"));
        assertEquals(true, res.getBoolean("ismoved"));
        assertEquals(42, res.getInt("blocksz"));
        assertEquals(true, res.getBoolean("retrievemode"));
        assertEquals("server1", res.getString("ownerreq"));
        assertEquals("server1", res.getString("requester"));
        assertEquals("server2", res.getString("requested"));
        assertEquals(Transfer.TASKSTEP.ERRORTASK.ordinal(), res.getInt("globalstep"));
        assertEquals(Transfer.TASKSTEP.TRANSFERTASK.ordinal(), res.getInt("globallaststep"));
        assertEquals(27, res.getInt("step"));
        assertEquals(ErrorCode.CompleteOk.code, res.getString("stepstatus").charAt(0));
        assertEquals(ErrorCode.Unknown.code, res.getString("infostatus").charAt(0));
        assertEquals(64, res.getInt("rank"));
        assertEquals(new Timestamp(192l), res.getTimestamp("starttrans"));
        assertEquals(new Timestamp(1511l), res.getTimestamp("stoptrans"));
        assertEquals(UpdatedInfo.TOSUBMIT.ordinal(), res.getInt("updatedInfo"));
    }


    @Test
    public void testFind() throws Exception {
        ArrayList<Filter> map = new ArrayList<Filter>();
        map.add(new Filter(DBTransferDAO.ID_RULE_FIELD, "=", "default"));
        map.add(new Filter(DBTransferDAO.OWNER_REQUEST_FIELD,"=", "server1"));

        TransferDAO dao = getDAO(getConnection());
        assertEquals(3, dao.find(map).size());
    }
}

