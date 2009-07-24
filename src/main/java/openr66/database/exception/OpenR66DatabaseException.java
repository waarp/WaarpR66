/**
 *
 */
package openr66.database.exception;

import openr66.protocol.exception.OpenR66Exception;

/**
 * Database exception
 *
 * @author frederic bregier
 */
public class OpenR66DatabaseException extends OpenR66Exception {

    /**
     *
     */
    private static final long serialVersionUID = 7656943570927304255L;

    /**
	 *
	 */
    public OpenR66DatabaseException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66DatabaseException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66DatabaseException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66DatabaseException(Throwable arg0) {
        super(arg0);
    }

}
