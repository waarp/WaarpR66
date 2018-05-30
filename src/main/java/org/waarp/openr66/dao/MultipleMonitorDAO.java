package org.waarp.openr66.dao;

import java.util.List;

import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.MultipleMonitor;

public interface MultipleMonitorDAO {
    List<MultipleMonitor> getAll() throws DAOException;
    MultipleMonitor get(String hostid) throws DAOException;
    boolean exist(String hostid) throws DAOException;
    void insert(MultipleMonitor multipleMonitor) throws DAOException;
    void update(MultipleMonitor multipleMonitor) throws DAOException;
    void delete(MultipleMonitor multipleMonitor) throws DAOException;
    void deleteAll() throws DAOException;
}

