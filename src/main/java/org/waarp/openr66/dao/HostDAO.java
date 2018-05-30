package org.waarp.openr66.dao;

import java.util.List;

import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Host;

public interface HostDAO {
    List<Host> getAll() throws DAOException;
    Host get(String hostid) throws DAOException;
    boolean exist(String hostid) throws DAOException;
    void insert(Host host) throws DAOException;
    void update(Host host) throws DAOException;
    void delete(Host host) throws DAOException;
    void deleteAll() throws DAOException;
}
