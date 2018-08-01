package org.waarp.openr66.dao;

import java.util.List;

import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Transfer;

/**
 * Interface to interact with Transfer objects in the persistance layer
 */
public interface TransferDAO {

    /**
     * Retrieve all Transfer object in a List from the persistance layer
     *
     * @throws DAOException If data access error occurs
     */
    List<Transfer> getAll() throws DAOException;

    /**
     * Retrieve the Transfer object with the specified Special ID from the persistance layer
     *
     * @param id special ID of the Transfer object requested
     * @throws DAOException If a data access error occurs
     */
    Transfer get(long id) throws DAOException;

    /**
     * Verify if a Transfer object with the specified Special ID exists in
     * the persistance layer
     *
     * @param id special ID of the Transfer object verified
     * @return true if a Transfer object with the specified Special ID exist; false
     * if no Transfer object correspond to the specified Special ID.
     * @throws DAOException If a data access error occurs
     */
    boolean exist(long id) throws DAOException;

    /**
     * Insert the specified Transfer object in the persistance layer
     *
     * @param transfer Transfer object to insert
     * @throws DAOException If a data access error occurs
     */
    void insert(Transfer transfer) throws DAOException;

    /**
     * Update the specified Transfer object in the persistance layer
     *
     * @param transfer Transfer object to update
     * @throws DAOException If a data access error occurs
     */
    void update(Transfer transfer) throws DAOException;

    /**
     * Remove the specified Transfer object from the persistance layer
     *
     * @param transfer Transfer object to insert
     * @throws DAOException If a data access error occurs
     */
    void delete(Transfer transfer) throws DAOException;

    /**
     * Remove all Transfer objects from the persistance layer
     *
     * @throws DAOException If a data access error occurs
     */
    void deleteAll() throws DAOException;
}
