/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License, or (at your option)
 * any later version.
 *
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package openr66.database.model;

import openr66.database.DbSession;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseNoDataException;
import openr66.database.exception.OpenR66DatabaseSqlError;

/**
 * Interface for Database Model
 *
 * This class is an interface for special functions that needs special implementations according to
 * the database model used.
 *
 * @author Frederic Bregier
 *
 */
public interface DbModel {
    /**
     * Create all necessary tables into the database
     * @throws OpenR66DatabaseNoConnectionError
     */
    public void createTables() throws OpenR66DatabaseNoConnectionError;

    /**
     * Reset the sequence for Runner SpecialIds
     * @throws OpenR66DatabaseNoConnectionError
     */
    public void resetSequence(long newvalue) throws OpenR66DatabaseNoConnectionError;

    /**
     * @param dbSession
     * @return The next unique specialId
     */
    public long nextSequence(DbSession dbSession)
            throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError,
            OpenR66DatabaseNoDataException;

    /**
     * Validate connection
     * @param dbSession
     * @throws OpenR66DatabaseNoConnectionError
     */
    public void validConnection(DbSession dbSession) throws OpenR66DatabaseNoConnectionError;

    /**
     * @param allfields string representing the equivalent to "*" in "select *" but more precisely
     *          as "field1, field2" in "select field1, field2"
     * @param request
     * @param limit
     * @return the new request String which will limit the result to the specified number of rows
     */
    public String limitRequest(String allfields, String request, int limit);
}
