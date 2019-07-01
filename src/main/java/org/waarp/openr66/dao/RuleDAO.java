package org.waarp.openr66.dao;

import java.util.List;

import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Rule;

/**
 * Interface to interact with Rule objects in the persistance layer
 */
public interface RuleDAO {

    /**
     * Retrieve all Rule objects in a List from the persistance layer
     *
     * @throws DAOException If data access error occurs
     */
    List<Rule> getAll() throws DAOException;

    /**
     * Retrieve all Rule objects corresponding to the given filters
     * a List from the persistance layer
     *
     * @param filters List of filter
     * @throws DAOException If data access error occurs
     */
    List<Rule> find(List<Filter> filters) throws DAOException;

    /**
     * Retrieve the Rule object with the specified Rulename from the
     * persistance layer
     *
     * @param rulename rulename of the Rule object requested
     * @throws DAOException If a data access error occurs
     */
    Rule select(String rulename) throws DAOException;

    /**
     * Verify if a Rule object with the specified Rulename exists in
     * the persistance layer
     *
     * @param rulename rulename of the Rule object verified
     * @return true if a Rule object with the specified Rulename exist; false
     * if no Rule object correspond to the specified Rulename.
     * @throws DAOException If a data access error occurs
     */
    boolean exist(String rulename) throws DAOException;

    /**
     * Insert the specified Rule object in the persistance layer
     *
     * @param rule Rule object to insert
     * @throws DAOException If a data access error occurs
     */
    void insert(Rule rule) throws DAOException;

    /**
     * Update the specified Rule object in the persistance layer
     *
     * @param rule Rule object to update
     * @throws DAOException If a data access error occurs
     */
    void update(Rule rule) throws DAOException;

    /**
     * Remove the specified Rule object from the persistance layer
     *
     * @param rule Rule object to insert
     * @throws DAOException If a data access error occurs
     */
    void delete(Rule rule) throws DAOException;

    /**
     * Remove all Rule objects from the persistance layer
     *
     * @throws DAOException If a data access error occurs
     */
    void deleteAll() throws DAOException;
}
