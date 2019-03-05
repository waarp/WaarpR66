package org.waarp.openr66.dao;

import java.util.List;

import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.MultipleMonitor;

/**
 * Interface to interact with MultipleMonitor objects in the persistance layer
 */
public interface MultipleMonitorDAO {

    /**
     * Retrieve all MultipleMonitor objects in a List from the persistance layer
     *
     * @throws DAOException If data access error occurs
     */
    List<MultipleMonitor> getAll() throws DAOException;

    /**
     * Retrieve all MultipleMonitor objects corresponding to the given filters
     * in a List from the persistance layer
     *
     * @param filters List of filter
     * @throws DAOException If data access error occurs
     */
    List<MultipleMonitor> find(List<Filter> filters) throws DAOException;

    /**
     * Retrieve the MultipleMonitor object with the specified hostid from the persistance layer
     *
     * @param hostid Hostid of the MultipleMonitor object requested
     * @throws DAOException If a data access error occurs
     */
    MultipleMonitor select(String hostid) throws DAOException;

    /**
     * Verify if a MultipleMonitor object with the specified hostid exists in
     * the persistance layer
     *
     * @param hostid Hostid of the MultipleMonitor object verified
     * @return true if a MultipleMonitor object with the specified hostid exist; false
     * if no MultipleMonitor object correspond to the specified hostid.
     * @throws DAOException If a data access error occurs
     */
    boolean exist(String hostid) throws DAOException;

    /**
     * Insert the specified MultipleMonitor object in the persistance layer
     *
     * @param multipleMonitor MultipleMonitor object to insert
     * @throws DAOException If a data access error occurs
     */
    void insert(MultipleMonitor multipleMonitor) throws DAOException;

    /**
     * Update the specified MultipleMonitor object in the persistance layer
     *
     * @param multipleMonitor MultipleMonitor object to update
     * @throws DAOException If a data access error occurs
     */
    void update(MultipleMonitor multipleMonitor) throws DAOException;

    /**
     * Remove the specified MultipleMonitor object from the persistance layer
     *
     * @param multipleMonitor MultipleMonitor object to insert
     * @throws DAOException If a data access error occurs
     */
    void delete(MultipleMonitor multipleMonitor) throws DAOException;

    /**
     * Remove all MultipleMonitor objects from the persistance layer
     *
     * @throws DAOException If a data access error occurs
     */
    void deleteAll() throws DAOException;

    void close();
}
