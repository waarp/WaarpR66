/**
 *
 */
package openr66.context.task.exception;

/**
 * Runner exception in error status
 * 
 * @author frederic bregier
 */
public class OpenR66RunnerErrorException extends OpenR66RunnerException {
    /**
     *
     */
    private static final long serialVersionUID = 3794468302790427511L;

    /**
	 *
	 */
    public OpenR66RunnerErrorException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66RunnerErrorException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66RunnerErrorException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66RunnerErrorException(Throwable arg0) {
        super(arg0);
    }

}
