package org.waarp.openr66.dao;

import java.util.List;

import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Transfer;

public interface TransferDAO {
    List<Transfer> getAll() throws DAOException; 
    Transfer get(long id) throws DAOException; 
    boolean exist(long id) throws DAOException; 
    void insert(Transfer transfer) throws DAOException; 
    void update(Transfer transfer) throws DAOException; 
    void delete(Transfer transfer) throws DAOException; 
    void deleteAll() throws DAOException; 
}
