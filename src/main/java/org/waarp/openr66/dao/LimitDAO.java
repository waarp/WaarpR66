package org.waarp.openr66.dao;

import java.util.List;

import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Limit;

/**
 * Interface to interact with Limit objects in the persistance layer
 */
public interface LimitDAO {

    /**
     * Retrieve all Limit objects in a List from the persistance layer
     *
     * @throws DAOException If data access error occurs
     */
    List<Limit> getAll() throws DAOException;

    /**
     * Retrieve all Limit objects correspondig to the given filters
     * in a List from the persistance layer
     *
     * @param filters List of filter
     * @throws DAOException If data access error occurs
     */
    List<Limit> find(List<Filter> filters) throws DAOException;
    /**
     * Retrieve the Limit object with the specified hostid from the persistance layer
     *
     * @param hostid Hostid of the Limit object requested
     * @throws DAOException If a data access error occurs
     */
    Limit select(String hostid) throws DAOException;

    /**
     * Verify if a Limit object with the specified hostid exists in
     * the persistance layer
     *
     * @param hostid Hostid of the Limit object verified
     * @return true if a Limit object with the specified hostid exist; false
     * if no Limit object correspond to the specified hostid.
     * @throws DAOException If a data access error occurs
     */
    boolean exist(String hostid) throws DAOException;

    /**
     * Insert the specified Limit object in the persistance layer
     *
     * @param limit Limit object to insert
     * @throws DAOException If a data access error occurs
     */
    void insert(Limit limit) throws DAOException;

    /**
     * Update the specified Limit object in the persistance layer
     *
     * @param limit Limit object to update
     * @throws DAOException If a data access error occurs
     */
    void update(Limit limit) throws DAOException;

    /**
     * Remove the specified Limit object from the persistance layer
     *
     * @param limit Limit object to insert
     * @throws DAOException If a data access error occurs
     */
    void delete(Limit limit) throws DAOException;

    /**
     * Remove all Limit objects from the persistance layer
     *
     * @throws DAOException If a data access error occurs
     */
    void deleteAll() throws DAOException;

    void close();
}
