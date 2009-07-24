/**
 *
 */
package openr66.context.task.exception;

import openr66.protocol.exception.OpenR66Exception;

/**
 * Runner exception
 *
 * @author frederic bregier
 */
public class OpenR66RunnerException extends OpenR66Exception {

    /**
     *
     */
    private static final long serialVersionUID = 5701631625487838804L;

    /**
	 *
	 */
    public OpenR66RunnerException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66RunnerException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66RunnerException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66RunnerException(Throwable arg0) {
        super(arg0);
    }

}
