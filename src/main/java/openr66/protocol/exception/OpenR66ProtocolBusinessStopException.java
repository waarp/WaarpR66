/**
 *
 */
package openr66.protocol.exception;

/**
 * Protocol exception on Business part
 *
 * @author frederic bregier
 */
public class OpenR66ProtocolBusinessStopException extends OpenR66ProtocolBusinessException {

    /**
     *
     */
    private static final long serialVersionUID = 8865871263523164597L;

    /**
	 *
	 */
    public OpenR66ProtocolBusinessStopException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolBusinessStopException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolBusinessStopException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolBusinessStopException(Throwable arg0) {
        super(arg0);
    }

}
