/**
 *
 */
package openr66.context.task.exception;

/**
 * Runner exception in end of tasks status
 *
 * @author frederic bregier
 */
public class OpenR66RunnerEndTasksException extends OpenR66RunnerException {

    /**
     *
     */
    private static final long serialVersionUID = -5410909604328960778L;

    /**
    *
    */
    public OpenR66RunnerEndTasksException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66RunnerEndTasksException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66RunnerEndTasksException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66RunnerEndTasksException(Throwable arg0) {
        super(arg0);
    }

}
