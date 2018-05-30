package org.waarp.openr66.dao;

import java.util.List;

import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Limit;

public interface LimitDAO {
    List<Limit> getAll() throws DAOException;
    Limit get(String hostid) throws DAOException;
    boolean exist(String hostid) throws DAOException;
    void insert(Limit limit) throws DAOException;
    void update(Limit limit) throws DAOException;
    void delete(Limit limit) throws DAOException;
    void deleteAll() throws DAOException;
}
