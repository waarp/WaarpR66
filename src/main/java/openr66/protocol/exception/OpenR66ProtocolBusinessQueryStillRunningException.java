/**
 *
 */
package openr66.protocol.exception;

/**
 * Protocol exception on Business part due to Query is still running
 *
 * @author frederic bregier
 */
public class OpenR66ProtocolBusinessQueryStillRunningException extends OpenR66ProtocolBusinessException {

    /**
     *
     */
    private static final long serialVersionUID = -2795883096275770203L;

    /**
	 *
	 */
    public OpenR66ProtocolBusinessQueryStillRunningException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolBusinessQueryStillRunningException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolBusinessQueryStillRunningException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolBusinessQueryStillRunningException(Throwable arg0) {
        super(arg0);
    }

}
