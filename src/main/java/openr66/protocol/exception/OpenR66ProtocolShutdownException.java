/**
 * 
 */
package openr66.protocol.exception;

/**
 * Protocol Exception to enable Shutdown
 * 
 * @author frederic bregier
 */
public class OpenR66ProtocolShutdownException extends OpenR66ProtocolException {

    /**
     * 
     */
    private static final long serialVersionUID = 9047867109141561841L;

    /**
	 * 
	 */
    public OpenR66ProtocolShutdownException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolShutdownException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolShutdownException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolShutdownException(Throwable arg0) {
        super(arg0);
    }

}
