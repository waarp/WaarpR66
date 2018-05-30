package org.waarp.openr66.dao;

import java.util.List;

import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Business;

public interface BusinessDAO {
    List<Business> getAll() throws DAOException;
    Business get(String hostid) throws DAOException;
    boolean exist(String hostid) throws DAOException;
    void insert(Business business) throws DAOException;
    void update(Business business) throws DAOException;
    void delete(Business business) throws DAOException;
    void deleteAll() throws DAOException;
}
