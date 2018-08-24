package org.waarp.openr66.dao.exception;

import java.lang.Exception;

import org.waarp.openr66.pojo.DataError;

public class DataException extends Exception {

    private DataError details;

    public DataException(String message) {
        super(message);
        details = new DataError();
    }

    public DataException(Throwable cause) {
        super(cause);
        details = new DataError();
    }

    public DataException(String message, Throwable cause) {
        super(message, cause);
        details = new DataError();
    }

    public DataException(String message, DataError details) {
        super(message);
        details = details;
    }

    public DataError getDetails() {
        return details;
    }
} 
