package org.waarp.openr66.dao;

import java.util.List;

import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Host;

/**
 * Interface to interact with Host objects in the persistance layer
 */
public interface HostDAO {

    /**
     * Retrieve all Host objects in a List from the persistance layer
     *
     * @throws DAOException If data access error occurs
     */
    List<Host> getAll() throws DAOException;

    /**
     * Retrieve all Host objects corresponding to the given filters
     * in a List from the persistance lsayer
     *
     * @param filters List of filter
     * @throws DAOException If data access error occurs
     */
    List<Host> find(List<Filter> filters) throws DAOException;
    /**
     * Retrieve the Host object with the specified hostid from the persistance layer
     *
     * @param hostid Hostid of the Host object requested
     * @throws DAOException If a data access error occurs
     */
    Host select(String hostid) throws DAOException;

    /**
     * Verify if a Host object with the specified hostid exists in
     * the persistance layer
     *
     * @param hostid Hostid of the Host object verified
     * @return true if a Host object with the specified hostid exist; false
     * if no Host object correspond to the specified hostid.
     * @throws DAOException If a data access error occurs
     */
    boolean exist(String hostid) throws DAOException;

    /**
     * Insert the specified Host object in the persistance layer
     *
     * @param host Host object to insert
     * @throws DAOException If a data access error occurs
     */
    void insert(Host host) throws DAOException;

    /**
     * Update the specified Host object in the persistance layer
     *
     * @param host Host object to update
     * @throws DAOException If a data access error occurs
     */
    void update(Host host) throws DAOException;

    /**
     * Remove the specified Host object from the persistance layer
     *
     * @param host Host object to insert
     * @throws DAOException If a data access error occurs
     */
    void delete(Host host) throws DAOException;

    /**
     * Remove all Host objects from the persistance layer
     *
     * @throws DAOException If a data access error occurs
     */
    void deleteAll() throws DAOException;
}
