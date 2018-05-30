package org.waarp.openr66.dao.exception;

import java.lang.Exception;
import java.lang.Throwable;

public class DAOException extends Exception {

    public DAOException(String message) {
        super(message);
    }

    public DAOException(Throwable cause) {
        super(cause);
    }

    public DAOException(String message, Throwable cause) {
        super(message, cause);
    }
} 
