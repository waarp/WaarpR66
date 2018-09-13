package org.waarp.openr66.configuration;

public class BadConfigurationException extends Exception {

    public BadConfigurationException(String message) {
        super(message);
    }

    public BadConfigurationException(Throwable cause) {
        super(cause);
    }

    public BadConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

}
