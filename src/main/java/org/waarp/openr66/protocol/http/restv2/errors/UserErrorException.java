/*
 * This file is part of Waarp Project (named also Waarp or GG).
 *
 * Copyright 2009, Waarp SAS, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * All Waarp Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 *
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * Waarp . If not, see <http://www.gnu.org/licenses/>.
 */

package org.waarp.openr66.protocol.http.restv2.errors;

import java.util.ArrayList;
import java.util.List;

/**
 * Thrown to indicate that the request made to the server is invalid, and lists
 * all the errors found as a list of {@link Error} objects.
 * Typically, these errors will be sent back as a '400 - Bad Request' HTTP response.
 */
public class UserErrorException extends RuntimeException {

    /**
     * The list of all {@link Error} errors found in the request.
     */
    public final List<Error> errors;

    /**
     * Initializes an exception with a single error.
     * @param error The error to add.
     */
    public UserErrorException(Error error) {
        this.errors = new ArrayList<Error>();
        errors.add(error);
    }

    /**
     * Initializes an exception with a list of errors.
     * @param errors The errors to add.
     */
    public UserErrorException(List<Error> errors) {
        this.errors = errors;
    }
}