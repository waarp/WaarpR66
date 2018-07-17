package org.waarp.openr66.dao.database.test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import org.waarp.openr66.dao.database.DBRuleDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Rule;;

public class DBRuleDAOTest {

    @Test
    public void TestConstructorNoConnection() {
        Connection connection = mock(Connection.class);

        DBRuleDAO tested = null;
        try {
            doThrow(SQLException.class).when(connection).prepareStatement(
                "SELECT * FROM RULES WHERE idrule = ?");

            tested = new DBRuleDAO(connection);
        } catch (SQLException e) {
            fail(e.toString());
        } catch (DAOException e) {
            assertEquals(tested, null);
        }
    }

    @Test
    public void TestConstructor() {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);

        DBRuleDAO tested = null;
        try {
            doReturn(statement).when(connection).prepareStatement(
                    anyString());

            tested = new DBRuleDAO(connection);
        } catch (SQLException e) {
            fail(e.toString());
        } catch (DAOException e) {
            fail(e.toString());
        }
        assertNotEquals(tested, null);
    }

    @Test
    public void TestGetRuleNoHostid() {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        
        Rule rule = null;
        try {
            doReturn(statement).when(connection).prepareStatement("SELECT * " +
                   "FROM RULES WHERE idrule = ?");
            doNothing().when(statement).setNString(0, "test");
            doReturn(resultSet).when(statement).executeQuery();
            doReturn(true).when(resultSet).next(); 
            doNothing().when(statement).close();
            doReturn("test").when(resultSet).getNString("idrule");
            doReturn("<hostids></hostids>").when(resultSet).getNString("hostids");
            doReturn(1).when(resultSet).getInt("modetrans");
            doReturn("recv").when(resultSet).getNString("recvpath");
            doReturn("send").when(resultSet).getNString("sendpath");
            doReturn("arch").when(resultSet).getNString("archivepath");
            doReturn("work").when(resultSet).getNString("workpath");
            doReturn("<tasks></tasks>").when(resultSet).getNString("rpretask");
            doReturn("<tasks></tasks>").when(resultSet).getNString("rposttask");
            doReturn("<tasks></tasks>").when(resultSet).getNString("rerrortask");
            doReturn("<tasks></tasks>").when(resultSet).getNString("spretask");
            doReturn("<tasks></tasks>").when(resultSet).getNString("sposttask");
            doReturn("<tasks></tasks>").when(resultSet).getNString("serrortask");

            DBRuleDAO tested = new DBRuleDAO(connection);
            rule = tested.get("test");
        } catch (SQLException e) {
            fail(e.toString());
        } catch (DAOException e) {
            fail(e.toString());
        }
        assertEquals("test", rule.getName());
        assertEquals(1, rule.getMode());
        assertEquals(0, rule.getHostids().size());
        assertEquals("recv", rule.getRecvPath());
        assertEquals("send", rule.getSendPath());
        assertEquals("arch", rule.getArchivePath());
        assertEquals("work", rule.getWorkPath());
        assertEquals(0, rule.getRPreTasks().size());
        assertEquals(0, rule.getRPostTasks().size());
        assertEquals(0, rule.getRErrorTasks().size());
        assertEquals(0, rule.getSPreTasks().size());
        assertEquals(0, rule.getSPostTasks().size());
        assertEquals(0, rule.getSErrorTasks().size());
    }

    @Test
    public void TestGetRuleOneHostid() {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        
        Rule rule = null;
        try {
            doReturn(statement).when(connection).prepareStatement("SELECT * " +
                   "FROM RULES WHERE idrule = ?");
            doNothing().when(statement).setNString(0, "test");
            doReturn(resultSet).when(statement).executeQuery();
            doReturn(true).when(resultSet).next(); 
            doNothing().when(statement).close();
            doReturn("test").when(resultSet).getNString("idrule");
            doReturn("<hostids><hostid>host1</hostid></hostids>").when(resultSet).getNString("hostids");
            doReturn(1).when(resultSet).getInt("modetrans");
            doReturn("recv").when(resultSet).getNString("recvpath");
            doReturn("send").when(resultSet).getNString("sendpath");
            doReturn("arch").when(resultSet).getNString("archivepath");
            doReturn("work").when(resultSet).getNString("workpath");
            doReturn("<tasks></tasks>").when(resultSet).getNString("rpretask");
            doReturn("<tasks></tasks>").when(resultSet).getNString("rposttask");
            doReturn("<tasks></tasks>").when(resultSet).getNString("rerrortask");
            doReturn("<tasks></tasks>").when(resultSet).getNString("spretask");
            doReturn("<tasks></tasks>").when(resultSet).getNString("sposttask");
            doReturn("<tasks></tasks>").when(resultSet).getNString("serrortask");

            DBRuleDAO tested = new DBRuleDAO(connection);
            rule = tested.get("test");
        } catch (SQLException e) {
            fail(e.toString());
        } catch (DAOException e) {
            fail(e.toString());
        }
        assertEquals("test", rule.getName());
        assertEquals(1, rule.getMode());
        assertEquals(1, rule.getHostids().size());
        assertEquals("host1", rule.getHostids().get(0));
        assertEquals("recv", rule.getRecvPath());
        assertEquals("send", rule.getSendPath());
        assertEquals("arch", rule.getArchivePath());
        assertEquals("work", rule.getWorkPath());
        assertEquals(0, rule.getRPreTasks().size());
        assertEquals(0, rule.getRPostTasks().size());
        assertEquals(0, rule.getRErrorTasks().size());
        assertEquals(0, rule.getSPreTasks().size());
        assertEquals(0, rule.getSPostTasks().size());
        assertEquals(0, rule.getSErrorTasks().size());
    }

    @Test
    public void TestGetRuleTwoHostidAndTasks() {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        
        Rule rule = null;
        try {
            doReturn(statement).when(connection).prepareStatement("SELECT * " +
                   "FROM RULES WHERE idrule = ?");
            doNothing().when(statement).setNString(0, "test");
            doReturn(resultSet).when(statement).executeQuery();
            doReturn(true).when(resultSet).next(); 
            doNothing().when(statement).close();
            doReturn("test").when(resultSet).getNString("idrule");
            doReturn("<hostids><hostid>host1</hostid><hostid>host2</hostid></hostids>").when(resultSet).getNString("hostids");
            doReturn(1).when(resultSet).getInt("modetrans");
            doReturn("recv").when(resultSet).getNString("recvpath");
            doReturn("send").when(resultSet).getNString("sendpath");
            doReturn("arch").when(resultSet).getNString("archivepath");
            doReturn("work").when(resultSet).getNString("workpath");
            doReturn("<tasks><task><type>Move</type><path>/tmp</path><delay>21</delay></task></tasks>").when(resultSet).getNString("rpretask");
            doReturn("<tasks><task><type>Move</type><path>/tmp</path><delay>22</delay></task><task><type>Exec</type><delay>33</delay><path>path</path></task></tasks>").when(resultSet).getNString("rposttask");
            doReturn("<tasks></tasks>").when(resultSet).getNString("rerrortask");
            doReturn("<tasks></tasks>").when(resultSet).getNString("spretask");
            doReturn("<tasks></tasks>").when(resultSet).getNString("sposttask");
            doReturn("<tasks></tasks>").when(resultSet).getNString("serrortask");

            DBRuleDAO tested = new DBRuleDAO(connection);
            rule = tested.get("test");
        } catch (SQLException e) {
            fail(e.toString());
        } catch (DAOException e) {
            fail(e.toString());
        }
        assertEquals("test", rule.getName());
        assertEquals(1, rule.getMode());
        assertEquals(2, rule.getHostids().size());
        assertEquals("host1", rule.getHostids().get(0));
        assertEquals("host2", rule.getHostids().get(1));
        assertEquals("recv", rule.getRecvPath());
        assertEquals("send", rule.getSendPath());
        assertEquals("arch", rule.getArchivePath());
        assertEquals("work", rule.getWorkPath());
        assertEquals(1, rule.getRPreTasks().size());
        assertEquals("Move", rule.getRPreTasks().get(0).getType());
        assertEquals("/tmp", rule.getRPreTasks().get(0).getPath());
        assertEquals(21, rule.getRPreTasks().get(0).getDelay());
        assertEquals("Move", rule.getRPostTasks().get(0).getType());
        assertEquals("/tmp", rule.getRPostTasks().get(0).getPath());
        assertEquals(22, rule.getRPostTasks().get(0).getDelay());
        assertEquals("Exec", rule.getRPostTasks().get(1).getType());
        assertEquals("path", rule.getRPostTasks().get(1).getPath());
        assertEquals(33, rule.getRPostTasks().get(1).getDelay());
        assertEquals(2, rule.getRPostTasks().size());
        assertEquals(0, rule.getRErrorTasks().size());
        assertEquals(0, rule.getSPreTasks().size());
        assertEquals(0, rule.getSPostTasks().size());
        assertEquals(0, rule.getSErrorTasks().size());
    }
}
