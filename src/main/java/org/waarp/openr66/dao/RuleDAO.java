package org.waarp.openr66.dao;

import java.util.List;

import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Rule;

public interface RuleDAO {
    List<Rule> getAll() throws DAOException; 
    Rule get(String id) throws DAOException;  
    boolean exist(String id) throws DAOException; 
    void insert(Rule rule) throws DAOException; 
    void update(Rule rule) throws DAOException; 
    void delete(Rule rule) throws DAOException; 
    void deleteAll() throws DAOException; 
}
